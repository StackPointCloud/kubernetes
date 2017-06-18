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
	"os"
	"path"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
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

// 	// NewUnmounter creates a new volume.Unmounter from recoverable state.
func (plugin *doVolumePlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, plugin.host.GetMounter())
}

// NewDeleter creates a new volume.Deleter which knows how to delete this
// resource in accordance with the underlying storage provider after the
// volume's release from a claim
NewDeleter(spec *Spec) (Deleter, error)
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

// Mounter

type doVolume struct {
	volName  string // TODO is this needed?
	podUID   types.UID
	volumeID string
	// Mounter interface that provides system calls to mount the global path to the pod local path.
	mounter mount.Interface
	plugin  *doVolumePlugin
	volume.MetricsNil
}

func (doVolume *doVolume) GetPath() string {
	name := doVolumePluginName
	return doVolume.plugin.host.GetPodVolumeDir(doVolume.podUID,
		utilstrings.EscapeQualifiedNameForDisk(name), doVolume.volName)
}

type doVolumeMounter struct {
	*doVolume
	// Filesystem type, optional.
	fsType string
	// Specifies whether the disk will be attached as read-only.
	readOnly bool
	// diskMounter provides the interface that is used to mount the actual volume device.
	diskMounter *mount.SafeFormatAndMount
}

var _ volume.Mounter = &doVolumeMounter{}

// GetAttributes returns the attributes of the mounter
func (b *doVolumeMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:        b.readOnly,
		Managed:         !b.readOnly,
		SupportsSELinux: true,
	}
}

// Checks prior to mount operations to verify that the required components (binaries, etc.)
// to mount the volume are available on the underlying node.
// If not, it returns an error
func (b *doVolumeMounter) CanMount() error {
	return nil
}

// SetUp attaches the disk and bind mounts to the volume path
func (b *doVolumeMounter) SetUp(fsGroup *types.UnixGroupID) error {
	return b.SetUpAt(b.GetPath(), fsGroup)
}

// SetUpAt attaches the disk and bind mounts to the volume path.
func (b *doVolumeMounter) SetUpAt(dir string, fsGroup *types.UnixGroupID) error {
	// TODO: handle failed mounts here.
	notMnt, err := b.mounter.IsLikelyNotMountPoint(dir)
	glog.V(4).Infof("Digital Ocean volume set up: %s %v %v", dir, !notMnt, err)
	if err != nil && !os.IsNotExist(err) {
		glog.Errorf("IsLikelyNotMountPoint failed validating mount point: %s %v", dir, err)
		return err
	}
	if !notMnt {
		return nil
	}

	globalPDPath := makeGlobalPDPath(b.plugin.host, b.volumeID)

	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	// Perform a bind mount to the full path to allow duplicate mounts of the same PD.
	options := []string{"bind"}
	if b.readOnly {
		options = append(options, "ro")
	}
	err = b.mounter.Mount(globalPDPath, dir, "", options)
	if err != nil {
		notMnt, mntErr := b.mounter.IsLikelyNotMountPoint(dir)
		if mntErr != nil {
			glog.Errorf("IsLikelyNotMountPoint failed validating mount point %s: %v", dir, mntErr)
			return err
		}
		if !notMnt {
			if mntErr = b.mounter.Unmount(dir); mntErr != nil {
				glog.Errorf("failed to unmount %s: %v", dir, mntErr)
				return err
			}
			notMnt, mntErr := b.mounter.IsLikelyNotMountPoint(dir)
			if mntErr != nil {
				glog.Errorf("IsLikelyNotMountPoint failed validating mount point %s: %v", dir, mntErr)
				return err
			}
			if !notMnt {
				// This is very odd, we don't expect it.  We'll try again next sync loop.
				glog.Errorf("%s is still mounted, despite call to unmount().  Will try again next sync loop.", dir)
				return err
			}
		}
		os.Remove(dir)
		glog.Errorf("Mount of disk %s failed: %v", dir, err)
		return err
	}

	if !b.readOnly {
		volume.SetVolumeOwnership(b, fsGroup)
	}

	glog.V(4).Infof("Digital Ocean volume %s successfully mounted to %s", b.volumeID, dir)
	return nil
}

type doVolumeUnmounter struct {
	*doVolume
}

var _ volume.Unmounter = &doVolumeUnmounter{}

// TearDown unmounts the bind mount
func (c *doVolumeUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

// TearDownAt unmounts the volume from the specified directory and
// removes traces of the SetUp procedure.
func (c *doVolumeUnmounter) TearDownAt(dir string) error {
	return util.UnmountPath(dir, c.mounter)
}

func makeGlobalPDPath(host volume.VolumeHost, volume string) string {
	return path.Join(host.GetPluginDir(doVolumePluginName), mount.MountsInGlobalPDPath, volume)
}
