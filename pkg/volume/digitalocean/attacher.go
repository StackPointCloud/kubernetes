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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/pkg/volume"
  "github.com/digitalocean/godo"
)

type doVolumeAttacher struct {
	// plugin     *doVolumePlugin
	host    volume.VolumeHost
	manager *DOManager
}

var _ volume.Attacher = &doVolumeAttacher{}

// // VolumesAreAttached checks whether the list of volumes still attached to the specified
// // node. It returns a map which maps from the volume spec to the checking result.
// // If an error is occurred during checking, the error will be returned
// VolumesAreAttached(specs []*Spec, nodeName types.NodeName) (map[*Spec]bool, error)
//
// // WaitForAttach blocks until the device is attached to this
// // node. If it successfully attaches, the path to the device
// // is returned. Otherwise, if the device does not attach after
// // the given timeout period, an error will be returned.
// WaitForAttach(spec *Spec, devicePath string, timeout time.Duration) (string, error)
//
// // GetDeviceMountPath returns a path where the device should
// // be mounted after it is attached. This is a global mount
// // point which should be bind mounted for individual volumes.
// GetDeviceMountPath(spec *Spec) (string, error)
//
// // MountDevice mounts the disk to a global path which
// // individual pods can then bind mount
// MountDevice(spec *Spec, devicePath string, deviceMountPath string) error

// Attaches the volume specified by the given spec to the node
func (attacher *doVolumeAttacher) Attach(spec *volume.Spec, nodeName types.NodeName) (string, error) {
	volumeSource, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

  // try to find droplet with same name as the kubernetes node
  var found *godo.Droplet
	droplets := attacher.manager.DropletList()
  for _, d := range droplets {
    if d.Name == nodeName {
      found = d
      break
    }
  }

  // if not found, look for other kubernetes properties
  // Internal IP seems to be our safest bet when names doesn't match
  if found == nil {
    node, err := attacher.nodeFromName(nodeName)
    for _, d := range droplets {
      for _, a := range node.Status.Addresses {
        if a.Type == v1.NodeInternalIP {
          ip, err = found.PrivateIPv4()
          if e != nil {
            return "", err
          }
          if ip == a.Address {
            found = d
            break
        }
      }
    }
    if found != nil {
      break
    }
  }

  if found == nil {
    return "" , fmt.Errorf("Couldn't match droplet.name to node name, nor droplet privateipv4 to kubernetes node internalip ")
  }

	// check if disk is already attached to node
  droplet := attacher.manager.GetDroplet(found.ID)

  // devicePath, err := attacher.awsVolumes.AttachDisk(volumeID, nodeName, readOnly)


	// if not attach to node

	// return mounted path

	// volumeSource.VolumeID
	//
	// // awsCloud.AttachDisk checks if disk is already attached to node and
	// // succeeds in that case, so no need to do that separately.
	// devicePath, err := attacher.awsVolumes.AttachDisk(volumeID, nodeName, readOnly)
	// if err != nil {
	// 	glog.Errorf("Error attaching volume %q to node %q: %+v", volumeSource.VolumeID, nodeName, err)
	// 	return "", err
	// }
	//
	// return devicePath, nil
	return "", nil
}

// func (attacher *awsElasticBlockStoreAttacher) VolumesAreAttached(specs []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {
//
// 	glog.Warningf("Attacher.VolumesAreAttached called for node %q - Please use BulkVerifyVolumes for AWS", nodeName)
// 	volumeNodeMap := map[types.NodeName][]*volume.Spec{
// 		nodeName: specs,
// 	}
// 	nodeVolumesResult := make(map[*volume.Spec]bool)
// 	nodesVerificationMap, err := attacher.BulkVerifyVolumes(volumeNodeMap)
// 	if err != nil {
// 		glog.Errorf("Attacher.VolumesAreAttached - error checking volumes for node %q with %v", nodeName, err)
// 		return nodeVolumesResult, err
// 	}
//
// 	if result, ok := nodesVerificationMap[nodeName]; ok {
// 		return result, nil
// 	}
// 	return nodeVolumesResult, nil
// }

// func (attacher *awsElasticBlockStoreAttacher) BulkVerifyVolumes(volumesByNode map[types.NodeName][]*volume.Spec) (map[types.NodeName]map[*volume.Spec]bool, error) {
// 	volumesAttachedCheck := make(map[types.NodeName]map[*volume.Spec]bool)
// 	diskNamesByNode := make(map[types.NodeName][]aws.KubernetesVolumeID)
// 	volumeSpecMap := make(map[aws.KubernetesVolumeID]*volume.Spec)
//
// 	for nodeName, volumeSpecs := range volumesByNode {
// 		for _, volumeSpec := range volumeSpecs {
// 			volumeSource, _, err := getVolumeSource(volumeSpec)
//
// 			if err != nil {
// 				glog.Errorf("Error getting volume (%q) source : %v", volumeSpec.Name(), err)
// 				continue
// 			}
//
// 			name := aws.KubernetesVolumeID(volumeSource.VolumeID)
// 			diskNamesByNode[nodeName] = append(diskNamesByNode[nodeName], name)
//
// 			nodeDisk, nodeDiskExists := volumesAttachedCheck[nodeName]
//
// 			if !nodeDiskExists {
// 				nodeDisk = make(map[*volume.Spec]bool)
// 			}
// 			nodeDisk[volumeSpec] = true
// 			volumeSpecMap[name] = volumeSpec
// 			volumesAttachedCheck[nodeName] = nodeDisk
// 		}
// 	}
// 	attachedResult, err := attacher.awsVolumes.DisksAreAttached(diskNamesByNode)
//
// 	if err != nil {
// 		glog.Errorf("Error checking if volumes are attached to nodes err = %v", err)
// 		return volumesAttachedCheck, err
// 	}
//
// 	for nodeName, nodeDisks := range attachedResult {
// 		for diskName, attached := range nodeDisks {
// 			if !attached {
// 				spec := volumeSpecMap[diskName]
// 				setNodeDisk(volumesAttachedCheck, spec, nodeName, false)
// 			}
// 		}
// 	}
//
// 	return volumesAttachedCheck, nil
// }
//
// func (attacher *awsElasticBlockStoreAttacher) WaitForAttach(spec *volume.Spec, devicePath string, timeout time.Duration) (string, error) {
// 	volumeSource, _, err := getVolumeSource(spec)
// 	if err != nil {
// 		return "", err
// 	}
//
// 	volumeID := volumeSource.VolumeID
// 	partition := ""
// 	if volumeSource.Partition != 0 {
// 		partition = strconv.Itoa(int(volumeSource.Partition))
// 	}
//
// 	if devicePath == "" {
// 		return "", fmt.Errorf("WaitForAttach failed for AWS Volume %q: devicePath is empty.", volumeID)
// 	}
//
// 	ticker := time.NewTicker(checkSleepDuration)
// 	defer ticker.Stop()
// 	timer := time.NewTimer(timeout)
// 	defer timer.Stop()
//
// 	for {
// 		select {
// 		case <-ticker.C:
// 			glog.V(5).Infof("Checking AWS Volume %q is attached.", volumeID)
// 			if devicePath != "" {
// 				devicePaths := getDiskByIdPaths(partition, devicePath)
// 				path, err := verifyDevicePath(devicePaths)
// 				if err != nil {
// 					// Log error, if any, and continue checking periodically. See issue #11321
// 					glog.Errorf("Error verifying AWS Volume (%q) is attached: %v", volumeID, err)
// 				} else if path != "" {
// 					// A device path has successfully been created for the PD
// 					glog.Infof("Successfully found attached AWS Volume %q.", volumeID)
// 					return path, nil
// 				}
// 			} else {
// 				glog.V(5).Infof("AWS Volume (%q) is not attached yet", volumeID)
// 			}
// 		case <-timer.C:
// 			return "", fmt.Errorf("Could not find attached AWS Volume %q. Timeout waiting for mount paths to be created.", volumeID)
// 		}
// 	}
// }
//
// func (attacher *awsElasticBlockStoreAttacher) GetDeviceMountPath(
// 	spec *volume.Spec) (string, error) {
// 	volumeSource, _, err := getVolumeSource(spec)
// 	if err != nil {
// 		return "", err
// 	}
//
// 	return makeGlobalPDPath(attacher.host, aws.KubernetesVolumeID(volumeSource.VolumeID)), nil
// }
//
// // FIXME: this method can be further pruned.
// func (attacher *awsElasticBlockStoreAttacher) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string) error {
// 	mounter := attacher.host.GetMounter()
// 	notMnt, err := mounter.IsLikelyNotMountPoint(deviceMountPath)
// 	if err != nil {
// 		if os.IsNotExist(err) {
// 			if err := os.MkdirAll(deviceMountPath, 0750); err != nil {
// 				return err
// 			}
// 			notMnt = true
// 		} else {
// 			return err
// 		}
// 	}
//
// 	volumeSource, readOnly, err := getVolumeSource(spec)
// 	if err != nil {
// 		return err
// 	}
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

// nodeNametoDroplet takes a node name and returns the droplet
func (attacher *doVolumeAttacher) nodeFromName(nodeName types.NodeName) (*v1.Node, error) {

	kubeClient, err := attacher.host.GetKubeClient()
	if err != nil {
		return nil, err
	}

	node, err := kubeClient.Core().Nodes().Get(nodeName, nil)
	if err != nil {
		return nil, err
	}

	node, err = ca.client.Core().Nodes().Get(node.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	return node, nil
}
