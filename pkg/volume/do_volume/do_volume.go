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
	"os"
	"path"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

type doVolume struct {
	volName  string // TODO is this needed?
	podUID   types.UID
	volumeID string
	mounter  mount.Interface
	plugin   *doVolumePlugin
	manager  *DOManager
	volume.MetricsNil
}

var _ volume.Volume = &doVolume{}

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

type doVolumeDeleter struct {
	*doVolume
}

var _ volume.Deleter = &doVolumeDeleter{}

func (d *doVolumeDeleter) GetPath() string {
	name := doVolumePluginName
	return doVolume.plugin.host.GetPodVolumeDir(doVolume.podUID,
		utilstrings.EscapeQualifiedNameForDisk(name), doVolume.volName)
}

func (d *doVolumeDeleter) Delete() error {

	err := d.manager.DeleteVolume(d.volumeID)
	if err != nil {
		glog.V(2).Infof("Error deleting Digital Ocean volume %s: %v", volumeID, err)
		return err
	}

	return nil
}
