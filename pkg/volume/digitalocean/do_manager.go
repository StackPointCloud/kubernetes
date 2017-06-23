/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package digitalocean

import (
	"fmt"
	"time"

	"github.com/digitalocean/godo"
	"github.com/digitalocean/godo/context"
	"github.com/golang/glog"
	"golang.org/x/oauth2"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	volumeAttachmentStatusConsecutiveErrorLimit = 10
	// waiting for attach/detach operation to complete. Starting with 10
	// seconds, multiplying by 1.2 with each step and taking 21 steps at maximum
	// it will time out after 31.11 minutes, which roughly corresponds to GCE
	// timeout (30 minutes).
	genericWaitTimeDelay         = 10 * time.Second
	volumeAttachmentStatusFactor = 1.2
	volumeAttachmentStatusSteps  = 21
)

// DOManager communicates with the DO API
type doManager struct {
	config  *DOManagerConfig
	client  *godo.Client
	context context.Context
}

// DOManagerConfig keeps Digital Ocean client configuration
type doManagerConfig struct {
	token  string
	region string
}

// TokenSource represents and oauth2 token source
type TokenSource struct {
	AccessToken string
}

// Token returns an oauth2 token
func (t *TokenSource) Token() (*oauth2.Token, error) {
	token := &oauth2.Token{
		AccessToken: t.AccessToken,
	}
	return token, nil
}

// NewDOManager returns a Digitial Ocean manager
func NewDOManager(token string) (*DOManager, error) {
	do := &doManager{}
	// generate client and test retrieving account info
	_, err := do.GetAccount()
	if err != nil {
		return nil, err
	}
	return do
}

// refreshDOClient will update the Digital Ocean client if it is not
// already cached
func (m *doManager) refreshDOClient() error {
	if m.context != nil && m.client != nil {
		return
	}
	if m.config.token == "" {
		return fmt.Errorf("DOManager needs to be initialized with a token")
	}
	if m.config.token == "" {
		return fmt.Errorf("DOManager needs to be initialized with the cluster region")
	}

	tokenSource := &TokenSource{
		AccessToken: m.token,
	}
	m.context = context.Background()
	oauthClient := oauth2.NewClient(m.context, tokenSource)
	m.client = godo.NewClient(oauthClient)
}

// removeDOClient will remove the cached Digital Ocean client
func (m *doManager) removeDOClient() {
	m.context = nil
	m.client = nil
}

// GetAccount returns the token related account
func (m *doManager) GetAccount() (*godo.Account, error) {
	m.refreshDOClient()
	account, _, err := m.client.Account.Get(m.ctx)
	if err != nil {
		m.removeDOClient()
		return nil, err
	}

	return account, nil
}

// GetDroplet retrieves the droplet by ID
func (m *doManager) GetDroplet(dropletID int) (*godo.Droplet, error) {
	m.refreshDOClient()
	droplet, _, err := m.client.Droplets.Get(m.ctx, dropletID)
	if err != nil {
		m.removeDOClient()
		return nil, err
	}
	return droplet, err
}

// DropletList return all droplets
func (m *doManager) DropletList() ([]godo.Droplet, error) {
	list := []godo.Droplet{}
	opt := &godo.ListOptions{}
	for {
		droplets, resp, err := m.client.Droplets.List(m.ctx, opt)
		if err != nil {
			m.removeDOClient()
			return nil, err
		}

		for _, d := range droplets {
			list = append(list, d)
		}
		if resp.Links == nil || resp.Links.IsLastPage() {
			break
		}
		page, err := resp.Links.CurrentPage()
		if err != nil {
			return nil, err
		}
		opt.Page = page + 1
	}
	return list, nil
}

func (m *doManager) GetVolume(volumeID string) (*godo.Volume, error) {
	vol, _, err := m.client.Storage.GetVolume(m.ctx, ID)
	if err != nil {
		m.removeDOClient()
		return nil, err
	}
	return vol, nil
}

// DeleteVolume deletes a Digital Ocean volume
func (m *doManager) DeleteVolume(volumeID string) error {
	m.refreshDOClient()
	_, err := m.client.Storage.DeleteVolume(m.ctx, volumeID)
	if err != nil {
		m.removeDOClient()
		return err
	}
	return nil
}

// CreateVolume creates a Digital Ocean volume from a provisioner and returns the ID
func (m *doManager) CreateVolume(name, description string, sizeGB int) (string, error) {
	m.refreshDOClient()

	req := &godo.VolumeCreateRequest{
		Region:        m.config.region,
		Name:          name,
		Description:   description,
		SizeGigaBytes: sizeGB,
	}

	vol, _, err := m.client.Storage.CreateVolume(m.ctx, req)
	if err != nil {
		m.removeDOClient()
		return "", 0, err
	}

	return vol.ID, nil
}

// AttachDisk attaches disk to given node
// returns the path the disk is being attached to
func (m *doManager) AttachDisk(volumeID string, dropletID, readOnly bool) (string, error) {
	vol, err := m.GetVolume(volumeID)
	if err != nil {
		m.removeDOClient()
		return "", err
	}

	needAttach := true
	for id := range vol.DropletIDs {
		if id == dropletID {
			needAttach = false
		}
	}

	if needAttach {
		action, _, err := m.client.StorageActions.Attach(m.ctx, volumeID, dropletID)
		if err != nil {
			return "", err
		}
		glog.V(2).Infof("AttachVolume volume=%q droplet=%q requested")
		err := m.WaitForVolumeAttach(volumeID, action.ID)
		if err != nil {
			return "", err
		}
	}
	return "/dev/disk/by-id/scsi-0DO" + vol.Name, nil
}

func (m *doManager) GetVolumeAction(volumeID string, actionID int) (*godo.Action, error) {
	action, _, err := m.client.StorageActions.Get(m.ctx, volumeID, actionID)
	if err != nil {
		m.removeDOClient()
		return nil, err
	}
	return action, nil
}

func (m *doManager) WaitForVolumeAttach(volumeID, actionID) error {
	backoff := wait.Backoff{
		Duration: volumeAttachmentStatusInitialDelay,
		Factor:   volumeAttachmentStatusFactor,
		Steps:    volumeAttachmentStatusSteps,
	}

	errorCount := 0
	err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		action, e := m.GetVolumeAction(volumeID, actionID)
		if e != nil {
			errorCount++
			if errorCount > volumeAttachmentStatusConsecutiveErrorLimit {
				return false, e
			} else {
				glog.Warningf("Ignoring error from get volume action; will retry: %q", e)
				return false, nil
			}
		} else {
			errorCount = 0
		}

		if action.Status != godo.ActionCompleted {
			glog.V(2).Infof("Waiting for volume %q state: actual=%s, desired=%s",
				volumeID, action.Status, godo.ActionCompleted)
			return false, nil
		}

		return true, nil
	})
	return err
}
