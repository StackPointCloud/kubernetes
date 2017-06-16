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

	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/volume"
)

const (
	doVolumePluginName = "kubernetes.io/do-volume"
)

// ProbeVolumePlugins is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&doVolumePlugin{}}
}

type doVolumePlugin struct {
	host volume.VolumeHost
}

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

// 	// ConstructVolumeSpec constructs a volume spec based on the given volume name
// 	// and mountPath. The spec may have incomplete information due to limited
// 	// information from input. This function is used by volume manager to reconstruct
// 	// volume spec by reading the volume directories from disk
// 	ConstructVolumeSpec(volumeName, mountPath string) (*Spec, error)
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
				DiskName: volumeID,
			},
		},
	}
	return volume.NewSpecFromVolume(doVolume), nil
}

// 	// NewMounter creates a new volume.Mounter from an API specification.
// 	// Ownership of the spec pointer in *not* transferred.
// 	// - spec: The v1.Volume spec
// 	// - pod: The enclosing pod
// 	NewMounter(spec *Spec, podRef *v1.Pod, opts VolumeOptions) (Mounter, error)
//
// 	// NewUnmounter creates a new volume.Unmounter from recoverable state.
// 	// - name: The volume name, as per the v1.Volume spec.
// 	// - podUID: The UID of the enclosing pod
// 	NewUnmounter(name string, podUID types.UID) (Unmounter, error)
// //
// // 	// ConstructVolumeSpec constructs a volume spec based on the given volume name
// // 	// and mountPath. The spec may have incomplete information due to limited
// // 	// information from input. This function is used by volume manager to reconstruct
// // 	// volume spec by reading the volume directories from disk
// // 	ConstructVolumeSpec(volumeName, mountPath string) (*Spec, error)
// //
// //
// //

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
