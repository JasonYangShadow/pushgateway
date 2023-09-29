// SPDX-FileCopyrightText: Copyright (c) 2023, CIQ, Inc. All rights reserved
// SPDX-License-Identifier: Apache-2.0
package monitor

import (
	"bytes"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/pushgateway/internal/cgroup"
	"github.com/prometheus/pushgateway/internal/cgroup/parser"
	"github.com/prometheus/pushgateway/internal/push"
	"github.com/prometheus/pushgateway/storage"
)

type MonitorInstance struct {
	*cgroup.CGroup
	ticker *time.Ticker

	ErrCh chan error
	Done  chan struct{}
}

func New(ticker *time.Ticker) *MonitorInstance {
	ins := &MonitorInstance{}
	ins.ticker = ticker
	ins.ErrCh = make(chan error, 1)
	ins.Done = make(chan struct{}, 1)
	return ins
}

func (i *MonitorInstance) Start(container *parser.ContainerInfo, ms storage.MetricStore, logger log.Logger) {
	c, err := cgroup.NewCGroup(container.Id)
	if err != nil {
		level.Error(logger).Log("msg", "while validating cgroup info", "err", err, "container id", container.Id)
		i.ErrCh <- err
		return
	}
	i.CGroup = c

	err = i.Apply(int(container.Pid))
	if err != nil {
		level.Error(logger).Log("msg", "while adding proc to cgroup info", "err", err, "container id", container.Id)
		i.ErrCh <- err
		return
	}

	defer i.Destroy()

	var buffer bytes.Buffer
	labels := make(map[string]string)
	labels["job"] = container.Id
	labels["path"] = container.FullPath
	labels["exe"] = container.Exe
	labels["pid"] = strconv.FormatUint(container.Pid, 10)
	labels["id"] = container.Id

	for {
		for range i.ticker.C {
			ok, err := i.HasProcess()
			if err != nil {
				level.Error(logger).Log("msg", "while verifying if there are any processes inside current cgroup", "err", err, "container id", container.Id)
				i.ErrCh <- err
				return
			}

			// No processes left in the current cgroup
			if !ok {
				level.Info(logger).Log("msg", "no processes in current cgroup, exit", "container id", container.Id)
				i.Done <- struct{}{}
				return
			}

			buffer.Reset()
			buffer, err := i.Marshal(&buffer)
			if err != nil {
				level.Error(logger).Log("msg", "while marshing the stat info", "err", err, "container id", container.Id)
				i.ErrCh <- err
				return
			}
			data := buffer.Bytes()

			// send request to pushgate
			err = push.Push(ms, data, labels)
			if err != nil {
				level.Error(logger).Log("msg", "while pushing data to pushgateway", "err", err, "container id", container.Id)
				i.ErrCh <- err
				return
			}
		}
	}
}
