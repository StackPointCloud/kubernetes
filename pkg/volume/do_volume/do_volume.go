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
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return volumeSource.VolumeID, nil
}

//
// 	// CanSupport tests whether the plugin supports a given volume
// 	// specification from the API.  The spec pointer should be considered
// 	// const.
// 	CanSupport(spec *Spec) bool
//
// 	// RequiresRemount returns true if this plugin requires mount calls to be
// 	// reexecuted. Atomically updating volumes, like Downward API, depend on
// 	// this to update the contents of the volume.
// 	RequiresRemount() bool
//
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
//
// 	// ConstructVolumeSpec constructs a volume spec based on the given volume name
// 	// and mountPath. The spec may have incomplete information due to limited
// 	// information from input. This function is used by volume manager to reconstruct
// 	// volume spec by reading the volume directories from disk
// 	ConstructVolumeSpec(volumeName, mountPath string) (*Spec, error)
//
// 	// SupportsMountOption returns true if volume plugins supports Mount options
// 	// Specifying mount options in a volume plugin that doesn't support
// 	// user specified mount options will result in error creating persistent volumes
// 	SupportsMountOption() bool
//
// 	// SupportsBulkVolumeVerification checks if volume plugin type is capable
// 	// of enabling bulk polling of all nodes. This can speed up verification of
// 	// attached volumes by quite a bit, but underlying pluging must support it.
// 	SupportsBulkVolumeVerification() bool
// }
//

func getVolumeSource(spec *volume.Spec) (*v1.DOVolumeVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.AWSElasticBlockStore != nil {
		return spec.Volume.AWSElasticBlockStore, spec.Volume.AWSElasticBlockStore.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.AWSElasticBlockStore != nil {
		return spec.PersistentVolume.Spec.AWSElasticBlockStore, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference an AWS EBS volume type")
}

// func getVolumeSource(
// 	spec *volume.Spec) (*v1.AWSElasticBlockStoreVolumeSource, bool, error) {
// 	if spec.Volume != nil && spec.Volume.AWSElasticBlockStore != nil {
// 		return spec.Volume.AWSElasticBlockStore, spec.Volume.AWSElasticBlockStore.ReadOnly, nil
// 	} else if spec.PersistentVolume != nil &&
// 		spec.PersistentVolume.Spec.AWSElasticBlockStore != nil {
// 		return spec.PersistentVolume.Spec.AWSElasticBlockStore, spec.ReadOnly, nil
// 	}
//
// 	return nil, false, fmt.Errorf("Spec does not reference an AWS EBS volume type")
// }
//
//
// func getVolumeSource(spec *volume.Spec) (*v1.AzureDiskVolumeSource, error) {
// 	if spec.Volume != nil && spec.Volume.AzureDisk != nil {
// 		return spec.Volume.AzureDisk, nil
// 	}
// 	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.AzureDisk != nil {
// 		return spec.PersistentVolume.Spec.AzureDisk, nil
// 	}
//
// 	return nil, fmt.Errorf("Spec does not reference an Azure disk volume type")
// }
//
// func getVolumeSource(spec *volume.Spec) (*v1.CephFSVolumeSource, bool, error) {
// 	if spec.Volume != nil && spec.Volume.CephFS != nil {
// 		return spec.Volume.CephFS, spec.Volume.CephFS.ReadOnly, nil
// 	} else if spec.PersistentVolume != nil &&
// 		spec.PersistentVolume.Spec.CephFS != nil {
// 		return spec.PersistentVolume.Spec.CephFS, spec.ReadOnly, nil
// 	}
//
// 	return nil, false, fmt.Errorf("Spec does not reference a CephFS volume type")
// }
