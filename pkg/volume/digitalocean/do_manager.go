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

	"github.com/digitalocean/godo"
	"github.com/digitalocean/godo/context"
	"golang.org/x/oauth2"
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
// func (c *doManager) AttachDisk(volumeID string, dropletID, readOnly bool) (string, error) {
// disk, err := newAWSDisk(c, diskName)
// if err != nil {
// 	m.removeDOClient()
// 	return "", err
// }

//
// 	awsInstance, info, err := c.getFullInstance(nodeName)
// 	if err != nil {
// 		return "", fmt.Errorf("error finding instance %s: %v", nodeName, err)
// 	}
//
// 	if readOnly {
// 		return "", errors.New("AWS volumes cannot be mounted read-only")
// 	}
//
// 	// mountDevice will hold the device where we should try to attach the disk
// 	var mountDevice mountDevice
// 	// alreadyAttached is true if we have already called AttachVolume on this disk
// 	var alreadyAttached bool
//
// 	// attachEnded is set to true if the attach operation completed
// 	// (successfully or not), and is thus no longer in progress
// 	attachEnded := false
// 	defer func() {
// 		if attachEnded {
// 			if !c.endAttaching(awsInstance, disk.awsID, mountDevice) {
// 				glog.Errorf("endAttaching called for disk %q when attach not in progress", disk.awsID)
// 			}
// 		}
// 	}()
//
// 	mountDevice, alreadyAttached, err = c.getMountDevice(awsInstance, info, disk.awsID, true)
// 	if err != nil {
// 		return "", err
// 	}
//
// 	// Inside the instance, the mountpoint always looks like /dev/xvdX (?)
// hostDevice := "/dev/xvd" + string(mountDevice)
// 	// We are using xvd names (so we are HVM only)
// 	// See http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
// 	ec2Device := "/dev/xvd" + string(mountDevice)
//
// 	if !alreadyAttached {
// 		request := &ec2.AttachVolumeInput{
// 			Device:     aws.String(ec2Device),
// 			InstanceId: aws.String(awsInstance.awsID),
// 			VolumeId:   disk.awsID.awsString(),
// 		}
//
// 		attachResponse, err := c.ec2.AttachVolume(request)
// 		if err != nil {
// 			attachEnded = true
// 			// TODO: Check if the volume was concurrently attached?
// 			return "", wrapAttachError(err, disk, awsInstance.awsID)
// 		}
// 		if da, ok := c.deviceAllocators[awsInstance.nodeName]; ok {
// 			da.Deprioritize(mountDevice)
// 		}
// 		glog.V(2).Infof("AttachVolume volume=%q instance=%q request returned %v", disk.awsID, awsInstance.awsID, attachResponse)
// 	}
//
// 	attachment, err := disk.waitForAttachmentStatus("attached")
// 	if err != nil {
// 		return "", err
// 	}
//
// 	// The attach operation has finished
// 	attachEnded = true
//
// 	// Double check the attachment to be 100% sure we attached the correct volume at the correct mountpoint
// 	// It could happen otherwise that we see the volume attached from a previous/separate AttachVolume call,
// 	// which could theoretically be against a different device (or even instance).
// 	if attachment == nil {
// 		// Impossible?
// 		return "", fmt.Errorf("unexpected state: attachment nil after attached %q to %q", diskName, nodeName)
// 	}
// 	if ec2Device != aws.StringValue(attachment.Device) {
// 		return "", fmt.Errorf("disk attachment of %q to %q failed: requested device %q but found %q", diskName, nodeName, ec2Device, aws.StringValue(attachment.Device))
// 	}
// 	if awsInstance.awsID != aws.StringValue(attachment.InstanceId) {
// 		return "", fmt.Errorf("disk attachment of %q to %q failed: requested instance %q but found %q", diskName, nodeName, awsInstance.awsID, aws.StringValue(attachment.InstanceId))
// 	}
//
//return hostDevice, nil
// 	return "", nil
// }
