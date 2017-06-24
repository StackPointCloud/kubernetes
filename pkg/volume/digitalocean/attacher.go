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
	"os"
	"time"

	"github.com/digitalocean/godo"
	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/volume"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

const (
	checkSleepDuration = time.Second
)

type doVolumeAttacher struct {
	// plugin     *doVolumePlugin
	host    volume.VolumeHost
	manager *doManager
}

var _ volume.Attacher = &doVolumeAttacher{}

// Attaches the volume specified by the given spec to the node
func (va *doVolumeAttacher) Attach(spec *volume.Spec, nodeName types.NodeName) (string, error) {
	volumeSource, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	found, err := va.findDroplet(string(nodeName))
	if err != nil {
		return "", err
	}

	// FIXME currently droplet lists don't fill volumes but they will
	// For the time being, we nned to retrieve the droplet
	droplet, err := va.manager.GetDroplet(found.ID)
	if err != nil {
		return "", err
	}

	devicePath, err := va.manager.AttachDisk(volumeSource.VolumeID, droplet.ID)
	if err != nil {
		return "", err
	}

	return devicePath, nil
}

// // VolumesAreAttached checks whether the list of volumes still attached to the specified node
func (va *doVolumeAttacher) VolumesAreAttached(specs []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {
	volumesAttachedCheck := make(map[*volume.Spec]bool)
	volumeSpecMap := make(map[string]*volume.Spec)
	volumeIDList := []string{}
	for _, spec := range specs {
		volumeSource, err := getVolumeSource(spec)
		if err != nil {
			glog.Errorf("Error getting volume (%q) source : %v", spec.Name(), err)
			continue
		}

		volumeIDList = append(volumeIDList, volumeSource.VolumeID)
		volumesAttachedCheck[spec] = true
		volumeSpecMap[volumeSource.VolumeID] = spec
	}

	droplet, err := va.findDroplet(string(nodeName))
	if err != nil {
		return nil, err
	}

	attachedResult, err := va.manager.DisksAreAttached(volumeIDList, droplet.ID)
	if err != nil {
		// Log error and continue with attach
		glog.Errorf(
			"Error checking if volumes (%v) are attached to current node (%q). err=%v",
			volumeIDList, nodeName, err)
		return volumesAttachedCheck, err
	}

	for volumeID, attached := range attachedResult {
		if !attached {
			spec := volumeSpecMap[volumeID]
			volumesAttachedCheck[spec] = false
			glog.V(2).Infof("VolumesAreAttached: check volume %q (specName: %q) is no longer attached", volumeID, spec.Name())
		}
	}
	return volumesAttachedCheck, nil
}

// // WaitForAttach blocks until the device is attached to this node
func (va *doVolumeAttacher) WaitForAttach(spec *volume.Spec, devicePath string, timeout time.Duration) (string, error) {
	volumeSource, err := getVolumeSource(spec)

	if err != nil {
		return "", err
	}

	if len(devicePath) == 0 {
		return "", fmt.Errorf("WaitForAttach failed for Digital Ocean volume %q: devicePath is empty.",
			volumeSource.VolumeID)
	}

	ticker := time.NewTicker(checkSleepDuration)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			glog.V(5).Infof("Checking if Digital Ocean Volume %q is attached.", volumeSource.VolumeID)

			if pathExists, err := volumeutil.PathExists(devicePath); err != nil {
				return "", fmt.Errorf("Error checking if path exists: %v", err)
			} else if pathExists {
				glog.Infof("Successfully found attached Digital Ocean Volume %q.", volumeSource.VolumeID)
				return devicePath, nil
			}
			glog.V(5).Infof("Digital Ocean Volume (%q) is not attached yet", volumeSource.VolumeID)
		case <-timer.C:
			return "", fmt.Errorf("Could not find attached Digital Ocean Volume %q. Timeout waiting for mount paths to be created.", volumeSource.VolumeID)
		}
	}
}

// GetDeviceMountPath returns a path where the device should
// be mounted after it is attached. This is a global mount
// point which should be bind mounted for individual volumes.
func (va *doVolumeAttacher) GetDeviceMountPath(spec *volume.Spec) (string, error) {
	volumeSource, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}
	return makeGlobalPDPath(va.host, volumeSource.VolumeID), nil
}

// // MountDevice mounts the disk to a global path which
func (va *doVolumeAttacher) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string) error {
	mounter := va.host.GetMounter()

	notMnt, err := mounter.IsLikelyNotMountPoint(deviceMountPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(deviceMountPath, 0750); err != nil {
				return err
			}
			notMnt = true
		} else {
			return err
		}
	}

	volumeSource, err := getVolumeSource(spec)
	if err != nil {
		return err
	}

	if notMnt {
		diskMounter := &mount.SafeFormatAndMount{Interface: mounter, Runner: exec.New()}
		mountOptions := volume.MountOptionFromSpec(spec)
		err = diskMounter.FormatAndMount(devicePath, deviceMountPath, volumeSource.FSType, mountOptions)
		if err != nil {
			os.Remove(deviceMountPath)
			return err
		}
	}

	return nil
}

//
// 	options := []string{}
// 	if readOnly {
// 		options = append(options, "ro")
// 	}
// 	if notMnt {
// 		diskMounter := &mount.SafeFormatAndMount{Interface: mounter, Runner: exec.New()}
// 		mountOptions := volume.MountOptionFromSpec(spec, options...)
// 		err = diskMounter.FormatAndMount(devicePath, deviceMountPath, volumeSource.FSType, mountOptions)
// 		if err != nil {
// 			os.Remove(deviceMountPath)
// 			return err
// 		}
// 	}
// 	return nil
// }

type doVolumeDetacher struct {
	host     volume.VolumeHost
	doVolume doVolume
	// mounter    mount.Interface
	// awsVolumes aws.Volumes
}

// func (detacher *awsElasticBlockStoreDetacher) Detach(deviceMountPath string, nodeName types.NodeName) error {
// 	volumeID := aws.KubernetesVolumeID(path.Base(deviceMountPath))
//
// 	attached, err := detacher.awsVolumes.DiskIsAttached(volumeID, nodeName)
// 	if err != nil {
// 		// Log error and continue with detach
// 		glog.Errorf(
// 			"Error checking if volume (%q) is already attached to current node (%q). Will continue and try detach anyway. err=%v",
// 			volumeID, nodeName, err)
// 	}
//
// 	if err == nil && !attached {
// 		// Volume is already detached from node.
// 		glog.Infof("detach operation was successful. volume %q is already detached from node %q.", volumeID, nodeName)
// 		return nil
// 	}
//
// 	if _, err = detacher.awsVolumes.DetachDisk(volumeID, nodeName); err != nil {
// 		glog.Errorf("Error detaching volumeID %q: %v", volumeID, err)
// 		return err
// 	}
// 	return nil
// }
//
// func (detacher *awsElasticBlockStoreDetacher) UnmountDevice(deviceMountPath string) error {
// 	return volumeutil.UnmountPath(deviceMountPath, detacher.mounter)
// }
//
// func setNodeDisk(
// 	nodeDiskMap map[types.NodeName]map[*volume.Spec]bool,
// 	volumeSpec *volume.Spec,
// 	nodeName types.NodeName,
// 	check bool) {
//
// 	volumeMap := nodeDiskMap[nodeName]
// 	if volumeMap == nil {
// 		volumeMap = make(map[*volume.Spec]bool)
// 		nodeDiskMap[nodeName] = volumeMap
// 	}
// 	volumeMap[volumeSpec] = check
// }

func (va *doVolumeAttacher) findDroplet(nodeName string) (*godo.Droplet, error) {

	// try to find droplet with same name as the kubernetes node
	droplets, err := va.manager.DropletList()

	for _, droplet := range droplets {
		if droplet.Name == nodeName {
			return &droplet, nil
		}
	}

	// if not found, look for other kubernetes properties
	// Internal IP seems to be our safest bet when names doesn't match
	node, err := va.nodeFromName(nodeName)
	if err != nil {
		return nil, err
	}

	for _, droplet := range droplets {
		for _, address := range node.Status.Addresses {
			if address.Type == v1.NodeInternalIP {
				ip, err := droplet.PrivateIPv4()
				if err != nil {
					return nil, err
				}
				if ip == address.Address {
					return &droplet, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("Couldn't match droplet name to node name, nor droplet private ip to node internal ip")
}

// nodeNametoDroplet takes a node name and returns the droplet
func (va *doVolumeAttacher) nodeFromName(nodeName string) (*v1.Node, error) {

	kubeClient := va.host.GetKubeClient()
	if kubeClient == nil {
		return nil, fmt.Errorf("Cannot get kube client")
	}

	node, err := kubeClient.Core().Nodes().Get(nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return node, nil
}
