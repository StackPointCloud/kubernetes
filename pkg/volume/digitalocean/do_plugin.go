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
	labelDropletName   = "stackpoint.io/instance_id"
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
var _ volume.ProvisionableVolumePlugin = &doVolumePlugin{}
var _ volume.AttachableVolumePlugin = &doVolumePlugin{}

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
	manager, err := plugin.createManager()
	if err != nil {
		return nil, err
	}
	return plugin.newDeleterInternal(spec, manager)
}

func (plugin *doVolumePlugin) NewProvisioner(options volume.VolumeOptions) (volume.Provisioner, error) {
	manager, err := plugin.createManager()
	if err != nil {
		return nil, err
	}
	return plugin.newProvisionerInternal(options, manager)
}

func (plugin *doVolumePlugin) NewAttacher() (volume.Attacher, error) {
	manager, err := plugin.createManager()
	if err != nil {
		return nil, err
	}
	return plugin.newAttacherInternal(manager)
}

func (plugin *doVolumePlugin) NewDetacher() (volume.Detacher, error) {
	manager, err := plugin.createManager()
	if err != nil {
		return nil, err
	}
	return plugin.newDetacherInternal(manager)
}

func (plugin *doVolumePlugin) GetDeviceMountRefs(deviceMountPath string) ([]string, error) {
	mounter := plugin.host.GetMounter()
	return mount.GetMountRefs(mounter, deviceMountPath)
}

func (plugin *doVolumePlugin) newAttacherInternal(manager *doManager) (volume.Attacher, error) {
	return &doVolumeAttacher{
		host:    plugin.host,
		manager: manager,
	}, nil
}

func (plugin *doVolumePlugin) newDetacherInternal(manager *doManager) (volume.Detacher, error) {
	return &doVolumeDetacher{
		host:    plugin.host,
		manager: manager,
	}, nil
}

func (plugin *doVolumePlugin) newProvisionerInternal(options volume.VolumeOptions, manager *doManager) (volume.Provisioner, error) {
	return &doVolumeProvisioner{
		doVolume: &doVolume{
			plugin:  plugin,
			manager: manager,
		},
		options: options,
	}, nil
}

func (plugin *doVolumePlugin) newDeleterInternal(spec *volume.Spec, manager *doManager) (volume.Deleter, error) {
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

	fsType := vol.FSType
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

func (plugin *doVolumePlugin) getDOToken() (*doManagerConfig, error) {
	secretMap, err := util.GetSecretForPV(secretNamespace, secretName, doVolumePluginName, plugin.host.GetKubeClient())
	if err != nil {
		return nil, err
	}

	token, ok := secretMap[secretToken]
	if !ok {
		return nil, fmt.Errorf("Missing \"%s\" in secret %s/%s", secretToken, secretNamespace, secretName)
	}

	region, ok := secretMap[secretRegion]
	if !ok {
		return nil, fmt.Errorf("Missing \"%s\" in secret %s/%s", secretRegion, secretNamespace, secretName)
	}

	return &doManagerConfig{
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

func (plugin *doVolumePlugin) createManager() (*doManager, error) {
	config, err := plugin.getDOToken()
	if err != nil {
		return nil, err
	}
	return newDOManager(config)
}
