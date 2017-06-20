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

package do_volume

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

// DeleteVolume deletes a Digital Ocean volume
func (m *doManager) DeleteVolume(volumeID string) error {
	m.refreshDOClient()
	_, err := m.client.Storage.DeleteVolume(m.ctx, volumeID)
	return err
}
