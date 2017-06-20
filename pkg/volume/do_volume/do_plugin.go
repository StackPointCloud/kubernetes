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

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

const (
	doVolumePluginName = "kubernetes.io/do-volume"
	secretNamespace    = "kube-system"
	secretName         = "digitalocean"
	secretToken        = "token"
	secretRegion       = "region"
)

// ProbeVolumePlugins is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&doVolumePlugin{}}
}

type doVolumePlugin struct {
	host volume.VolumeHost
}

var _ volume.VolumePlugin = &doVolumePlugin{}
var _ volume.PersistentVolumePlugin = &doVolumePlugin{}
var _ volume.DeletableVolumePlugin = &doVolumePlugin{}

// var _ volume.ProvisionableVolumePlugin = &doVolumePlugin{}

// Init initializes the plugin
func (plugin *doVolumePlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

func (plugin *doVolumePlugin) GetPluginName() string {
	return doVolumePluginName
}

// GetVolumeName returns the ID to uniquely identifying the volume spec.
func (plugin *doVolumePlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return volumeSource.VolumeID, nil
}

// CanSupport returns a boolen that indicates if the volume is supported
func (plugin *doVolumePlugin) CanSupport(spec *volume.Spec) bool {
	return (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.DOVolume != nil) ||
		(spec.Volume != nil && spec.PersistentVolume.Spec.DOVolume != nil)
}

// RequiresRemount returns false if this plugin desn't need re-mount
func (plugin *doVolumePlugin) RequiresRemount() bool {
	return false
}

// SupportsMountOption returns false if volume plugin don't supports Mount options
func (plugin *doVolumePlugin) SupportsMountOption() bool {
	return false
}

// SupportsBulkVolumeVerification checks if volume plugin allows bulk volume verification
func (plugin *doVolumePlugin) SupportsBulkVolumeVerification() bool {
	return false
}

func (plugin *doVolumePlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
	}
}

// ConstructVolumeSpec constructs a volume spec based on name and path
func (plugin *doVolumePlugin) ConstructVolumeSpec(volName, mountPath string) (*volume.Spec, error) {
	mounter := plugin.host.GetMounter()
	pluginDir := plugin.host.GetPluginDir(plugin.GetPluginName())
	volumeID, err := mounter.GetDeviceNameFromMount(mountPath, pluginDir)
	if err != nil {
		return nil, err
	}
	doVolume := &v1.Volume{
		Name: volName,
		VolumeSource: v1.VolumeSource{
			DOVolume: &v1.DOVolumeSource{
				VolumeID: volumeID,
			},
		},
	}
	return volume.NewSpecFromVolume(doVolume), nil
}

// NewMounter creates a new volume.Mounter from an API specification
func (plugin *doVolumePlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {
	return plugin.newMounterInternal(spec, pod.UID, plugin.host.GetMounter())
}

// NewUnmounter creates a new volume.Unmounter from recoverable state.
func (plugin *doVolumePlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, plugin.host.GetMounter())
}

// NewDeleter creates a new volume.Deleter which knows how to delete this resource
func (plugin *doVolumePlugin) NewDeleter(spec *volume.Spec) (volume.Deleter, error) {
	config, err := plugin.getDOToken()
	if e != nil {
		return nil, e
	}
	manager = NewDOManager(config)

	return plugin.newDeleterInternal(spec, manager)
}

func (plugin *doVolumePlugin) newMDeleterInternal(spec *volume.Spec, manager *doManager) (volume.Deleter, error) {
	vol, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	return &doVolumeDeleter{
		doVolume: &doVolume{
			volumeID: vol.VolumeID,
			plugin:   plugin,
			manager:  manager,
		},
	}, nil
}

func (plugin *doVolumePlugin) newMounterInternal(spec *volume.Spec, podUID types.UID, mounter mount.Interface) (volume.Mounter, error) {
	vol, err := getVolumeSource(spec)
	if err != nil {
		return nil, err
	}

	fsType = vol.FSType
	readOnly := vol.ReadOnly
	volID := vol.VolumeID

	return &doVolumeMounter{
		doVolume: &doVolume{
			volName:  spec.Name(),
			podUID:   podUID,
			volumeID: volID,
			mounter:  mounter,
			plugin:   plugin,
		},
		fsType:      fsType,
		readOnly:    readOnly,
		diskMounter: &mount.SafeFormatAndMount{Interface: plugin.host.GetMounter(), Runner: exec.New()},
	}, nil
}

func (plugin *doVolumePlugin) newUnmounterInternal(volName string, podUID types.UID, mounter mount.Interface) (volume.Unmounter, error) {
	return &doVolumeUnmounter{
		&doVolume{
			volName: volName,
			podUID:  podUID,
			mounter: mounter,
			plugin:  plugin,
		},
	}, nil
}

func (plugin *doVolumePlugin) getDOToken() (*DOManagerConfig, error) {
	secretMap, err := util.GetSecretForPV(secretNamespace, secretName, doVolumePluginName, plugin.host.GetKubeClient())
	if err != nil {
		return nil, err
	}

	if token, ok = secretMap[secretToken]; !ok {
		return nil, fmt.Errorf("Missing \"%s\" in secret %s/%s", secretToken, secretNamespace, secretName)
	}

	if region, ok = secretMap[secretRegion]; !ok {
		return nil, fmt.Errorf("Missing \"%s\" in secret %s/%s", secretRegion, secretNamespace, secretName)
	}

	return &DOManagerConfig{
		token:  token,
		region: region,
	}, nil
}

func getVolumeSource(spec *volume.Spec) (*v1.DOVolumeSource, error) {
	if spec.Volume != nil && spec.Volume.DOVolume != nil {
		return spec.Volume.DOVolume, nil
	}
	if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.DOVolume != nil {
		return spec.PersistentVolume.Spec.DOVolume, nil
	}

	return nil, fmt.Errorf("Spec does not reference a Digital Ocean disk volume type")
}
