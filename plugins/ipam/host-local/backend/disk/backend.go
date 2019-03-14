// Copyright 2015 CNI authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disk

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/containernetworking/plugins/plugins/ipam/host-local/backend"
	"runtime"
)

const lastIPFilePrefix = "last_reserved_ip."

var defaultDataDir = "/var/lib/cni/networks"

// Store is a simple disk-backed store that creates one file per IP
// address in a given directory. The contents of the file are the container ID.
type Store struct {
	*FileLock
	dataDir string
}

// Store implements the Store interface
var _ backend.Store = &Store{}

func New(network, dataDir string) (*Store, error) {
	if dataDir == "" {
		dataDir = defaultDataDir
	}
	dir := filepath.Join(dataDir, network)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	lk, err := NewFileLock(dir)
	if err != nil {
		return nil, err
	}
	return &Store{lk, dir}, nil
}

func (s *Store) Reserve(id string, ip net.IP, rangeID string) (bool, error) {
	fname := GetEscapedPath(s.dataDir, ip.String())

	f, err := os.OpenFile(fname, os.O_RDWR|os.O_EXCL|os.O_CREATE, 0644)
	if os.IsExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if _, err := f.WriteString(strings.TrimSpace(id)); err != nil {
		f.Close()
		os.Remove(fname)
		return false, err
	}
	if err := f.Close(); err != nil {
		os.Remove(fname)
		return false, err
	}
	// store the reserved ip in lastIPFile
	ipfile := GetEscapedPath(s.dataDir, lastIPFilePrefix+rangeID)
	err = ioutil.WriteFile(ipfile, []byte(ip.String()), 0644)
	if err != nil {
		return false, err
	}
	return true, nil
}

// LastReservedIP returns the last reserved IP if exists
func (s *Store) LastReservedIP(rangeID string) (net.IP, error) {
	ipfile := GetEscapedPath(s.dataDir, lastIPFilePrefix+rangeID)
	data, err := ioutil.ReadFile(ipfile)
	if err != nil {
		return nil, err
	}
	return net.ParseIP(string(data)), nil
}

func (s *Store) Release(ip net.IP) error {
	return os.Remove(GetEscapedPath(s.dataDir, ip.String()))
}

// N.B. This function eats errors to be tolerant and
// release as much as possible
func (s *Store) ReleaseByID(id string) error {
	err := filepath.Walk(s.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return nil
		}
		if strings.TrimSpace(string(data)) == strings.TrimSpace(id) {
			if err := os.Remove(path); err != nil {
				return nil
			}
		}
		return nil
	})
	return err
}

func GetEscapedPath(dataDir string, fname string) string {
	if runtime.GOOS == "windows" {
		fname = strings.Replace(fname, ":", "_", -1)
	}
	return filepath.Join(dataDir, fname)
}

// edge k8s: HasReservedIP verify the pod already had reserved ip or not.
// and return the reserved ip on the other hand.
func (s *Store) HasReservedIP(podNs, podName string) (bool, net.IP) {
	ip := net.IP{}
	if len(podName) == 0 {
		return false, ip
	}

	// Pod, ip mapping info are recorded with file name: PodIP_PodNs_PodName
	podIPNsNameFileName, err := s.findPodFileName("", podNs, podName)
	if err != nil {
		return false, ip
	}

	if len(podIPNsNameFileName) != 0 {
		ipStr, ns, name := resolvePodFileName(podIPNsNameFileName)
		if ns == podNs && name == podName {
			ip = net.ParseIP(ipStr)
			if ip != nil {
				return true, ip
			}
		}
	}

	return false, ip
}

// edge k8s: ReservePodInfo create podName file for storing ip or update ip file with container id
// in terms of podIPIsExist
func (s *Store) ReservePodInfo(id string, ip net.IP, podNs, podName string, podIPIsExist bool) (bool, error) {
	if podIPIsExist {
		// pod Ns/Name file is exist, update ip file with new container id.
		fname := GetEscapedPath(s.dataDir, ip.String())
		err := ioutil.WriteFile(fname, []byte(strings.TrimSpace(id)), 0644)
		if err != nil {
			return false, err
		}
	} else {
		// for new pod, create a new file named "PodIP_PodNs_PodName",
		// if there is already file named with prefix "ip_", rename the old file with new PodNs and PodName.
		if len(podName) != 0 {
			podIPNsNameFile := GetEscapedPath(s.dataDir, podFileName(ip.String(), podNs, podName))
			podIPNsNameFileName, err := s.findPodFileName(ip.String(), "", "")
			if err != nil {
				return false, err
			}

			if len(podIPNsNameFileName) != 0 {
				oldPodIPNsNameFile := GetEscapedPath(s.dataDir, podIPNsNameFileName)
				err = os.Rename(oldPodIPNsNameFile, podIPNsNameFile)
				if err != nil {
					return false, err
				} else {
					return true, nil
				}
			}

			err = ioutil.WriteFile(podIPNsNameFile, []byte{}, 0644)
			if err != nil {
				return false, err
			}
		}
	}

	return true, nil
}

func podFileName(ip, ns, name string) string {
	if len(ip) != 0 && len(ns) != 0 {
		return fmt.Sprintf("%s_%s_%s", ip, ns, name)
	}

	return name
}

func resolvePodFileName(fName string) (ip, ns, name string) {
	parts := strings.Split(fName, "_")
	if len(parts) == 3 {
		ip = parts[0]
		ns = parts[1]
		name = parts[2]
	}

	return
}

func (s *Store) findPodFileName(ip, ns, name string) (string, error) {
	var pattern string
	if len(ip) != 0 {
		pattern = fmt.Sprintf("%s_*", ip)
	} else if len(ns) != 0 && len(name) != 0 {
		pattern = fmt.Sprintf("*_%s_%s", ns, name)
	} else {
		return "", nil
	}
	pattern = GetEscapedPath(s.dataDir, pattern)

	podFiles, err := filepath.Glob(pattern)
	if err != nil {
		return "", err
	}

	if len(podFiles) == 1 {
		_, fName := filepath.Split(podFiles[0])
		if strings.Count(fName, "_") == 2 {
			return fName, nil
		}
	}

	return "", nil
}
