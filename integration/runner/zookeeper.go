/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package runner

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/pkg/errors"
	"github.com/tedsuo/ifrit"
)

const ZookeeperDefaultImage = "hyperledger/fabric-zookeeper:latest"

type Zookeeper struct {
	Client         *docker.Client
	Image          string
	HostIP         string
	HostPort       []int
	ContainerPorts []docker.Port
	Name           string
	StartTimeout   time.Duration

	NetworkID   string
	NetworkName string
	ClientPort  docker.Port
	LeaderPort  docker.Port
	PeerPort    docker.Port
	ZooMyID     int
	ZooServers  string

	ErrorStream  io.Writer
	OutputStream io.Writer

	containerID      string
	hostAddress      string
	containerAddress string
	address          string

	mutex   sync.Mutex
	stopped bool
}

func (z *Zookeeper) Run(sigCh <-chan os.Signal, ready chan<- struct{}) error {
	if z.Image == "" {
		z.Image = ZookeeperDefaultImage
	}

	if z.Name == "" {
		z.Name = DefaultNamer()
	}

	if z.HostIP == "" {
		z.HostIP = "127.0.0.1"
	}

	if z.ContainerPorts == nil {
		if z.ClientPort == docker.Port("") {
			z.ClientPort = docker.Port("2181/tcp")
		}
		if z.LeaderPort == docker.Port("") {
			z.LeaderPort = docker.Port("3888/tcp")
		}
		if z.PeerPort == docker.Port("") {
			z.PeerPort = docker.Port("2888/tcp")
		}

		z.ContainerPorts = []docker.Port{
			z.ClientPort,
			z.LeaderPort,
			z.PeerPort,
		}
	}

	if z.StartTimeout == 0 {
		z.StartTimeout = DefaultStartTimeout
	}

	if z.ZooMyID == 0 {
		z.ZooMyID = 1
	}

	if z.Client == nil {
		client, err := docker.NewClientFromEnv()
		if err != nil {
			return err
		}
		z.Client = client
	}

	config := &docker.Config{
		Image: z.Image,
		Env: []string{
			fmt.Sprintf("ZOO_MY_ID=%d", z.ZooMyID),
			fmt.Sprintf("ZOO_SERVERS=%s", z.ZooServers),
		},
	}

	containerOptions := docker.CreateContainerOptions{
		Name: z.Name,
		HostConfig: &docker.HostConfig{
			AutoRemove: true,
		},
		Config: config,
	}

	if z.NetworkName != "" && z.NetworkID != "" {
		networkingConfig := &docker.NetworkingConfig{
			EndpointsConfig: map[string]*docker.EndpointConfig{
				z.NetworkName: &docker.EndpointConfig{
					NetworkID: z.NetworkID,
				},
			},
		}

		containerOptions.NetworkingConfig = networkingConfig
	}

	container, err := z.Client.CreateContainer(containerOptions)
	if err != nil {
		return err
	}
	z.containerID = container.ID

	err = z.Client.StartContainer(container.ID, nil)
	if err != nil {
		return err
	}
	defer z.Stop()

	container, err = z.Client.InspectContainer(container.ID)
	if err != nil {
		return err
	}

	z.containerAddress = net.JoinHostPort(
		container.NetworkSettings.IPAddress,
		z.ContainerPorts[0].Port(),
	)

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	go z.streamLogs(streamCtx)

	containerExit := z.wait()
	ctx, cancel := context.WithTimeout(context.Background(), z.StartTimeout)
	defer cancel()

	select {
	case <-ctx.Done():
		return errors.Wrapf(ctx.Err(), "zookeeper in container %s did not start", z.containerID)
	case <-containerExit:
		return errors.New("container exited before ready")
	default:
		z.address = z.containerAddress
	}

	close(ready)

	select {
	case err := <-containerExit:
		return err
	case <-sigCh:
		return z.Stop()
	}
}

func (z *Zookeeper) wait() <-chan error {
	exitCh := make(chan error)
	go func() {
		if _, err := z.Client.WaitContainer(z.containerID); err != nil {
			exitCh <- err
		}
	}()

	return exitCh
}

func (z *Zookeeper) streamLogs(ctx context.Context) error {
	if z.ErrorStream == nil && z.OutputStream == nil {
		return nil
	}

	logOptions := docker.LogsOptions{
		Context:      ctx,
		Container:    z.ContainerID(),
		ErrorStream:  z.ErrorStream,
		OutputStream: z.OutputStream,
		Stderr:       z.ErrorStream != nil,
		Stdout:       z.OutputStream != nil,
		Follow:       true,
	}
	return z.Client.Logs(logOptions)
}

func (z *Zookeeper) ContainerID() string {
	return z.containerID
}

func (z *Zookeeper) ContainerAddress() string {
	return z.containerAddress
}

func (z *Zookeeper) Start() error {
	p := ifrit.Invoke(z)

	select {
	case <-p.Ready():
		return nil
	case err := <-p.Wait():
		return err
	}
}

func (z *Zookeeper) Stop() error {
	z.mutex.Lock()
	if z.stopped {
		z.mutex.Unlock()
		return errors.Errorf("container %s already stopped", z.Name)
	}
	z.stopped = true
	z.mutex.Unlock()

	err := z.Client.StopContainer(z.containerID, 0)
	if err != nil {
		return err
	}

	_, err = z.Client.PruneVolumes(docker.PruneVolumesOptions{})
	return err
}
