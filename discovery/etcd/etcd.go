// Copyright 2016 Andrew Chernov
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"

	etcdCli "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

const (
	// Constants for instrumentation.
	namespace = "prometheus"
)

var (
	etcdFailuresCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sd_etcd_failures_total",
			Help:      "The number of Etcd call failures.",
		},
		[]string{"call"},
	)

	etcdDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: namespace,
			Name:      "sd_etcd_duration_seconds",
			Help:      "The duration of a Etcd call in seconds.",
		},
		[]string{"call"},
	)
)

func init() {
	prometheus.MustRegister(etcdFailuresCount)
	prometheus.MustRegister(etcdDuration)

	// Initialize metric vectors.
	etcdFailuresCount.WithLabelValues("get")
	etcdFailuresCount.WithLabelValues("next")

	etcdDuration.WithLabelValues("get")
	etcdDuration.WithLabelValues("next")
}

// MetricInfo contains info where monitoring gets metrics from node
type MetricInfo struct {
	URL  string      `json:"url"`
	Tags []MetricTag `json:"tags"`
}

// MetricTag tags for host using for monitoring
type MetricTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type EtcdDiscovery struct {
	client        etcdCli.Client
	kapi          etcdCli.KeysAPI
	nodes         map[string]string
	keyPrefix     string
	metricKey     string
	retryInterval model.Duration
}

// NewEtcdDiscovery returns a new EtcdDiscovery for the given config.
func NewEtcdDiscovery(conf *config.EtcdSDConfig) (*EtcdDiscovery, error) {
	etcdConf := etcdCli.Config{
		Endpoints: conf.Endpoints,
		Username:  conf.Username,
		Password:  conf.Password,
	}

	etcdClient, err := etcdCli.New(etcdConf)
	if err != nil {
		return nil, err
	}

	ed := &EtcdDiscovery{
		client:        etcdClient,
		kapi:          etcdCli.NewKeysAPI(etcdClient),
		keyPrefix:     conf.KeyPrefix,
		metricKey:     conf.MetricKey,
		retryInterval: conf.RetryInterval,
	}

	return ed, nil
}

// Run implements the TargetProvider interface.
func (ed *EtcdDiscovery) Run(ctx context.Context, ch chan<- []*config.TargetGroup) {
	defer close(ch)

	for {
		if err := ed.watchNodes(ctx, ch); err != nil {
			log.Error(err.Error())
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(ed.retryInterval)):
		}
	}
}

func (ed *EtcdDiscovery) watchNodes(ctx context.Context, ch chan<- []*config.TargetGroup) error {
	index, err := ed.fetchNodes(ctx)
	if err != nil {
		return err
	}

	targetGroups, err := ed.makeTargetGroupsForAllNodes()
	if err != nil {
		return err
	}

	ch <- targetGroups

	// create watcher
	watcher := ed.kapi.Watcher(ed.keyPrefix, &etcdCli.WatcherOptions{
		AfterIndex: index,
		Recursive:  true,
	})

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		t0 := time.Now()
		wresp, err := watcher.Next(ctx)
		etcdDuration.WithLabelValues("next").Observe(time.Since(t0).Seconds())

		if err != nil {
			etcdFailuresCount.WithLabelValues("next").Inc()
			return fmt.Errorf("watch key %q endpoints: %v error: %s",
				ed.keyPrefix, ed.client.Endpoints(), err.Error())
		}

		log.Debugf("watcher.next: %q => %q", wresp.Node.Key, wresp.Node.Value)

		if !strings.Contains(wresp.Node.Key, ed.metricKey) {
			continue
		}

		var changedKey string
		switch wresp.Action {
		case "set", "update", "create", "compareAndSwap":
			v, exists := ed.nodes[wresp.Node.Key]
			if !exists || v != wresp.Node.Value {
				ed.nodes[wresp.Node.Key] = wresp.Node.Value
				changedKey = wresp.Node.Key
			}

		case "delete", "compareAndDelete", "expire":
			delete(ed.nodes, wresp.Node.Key)
			changedKey = wresp.Node.Key

		default:
			return fmt.Errorf("unknown action type: '%s'", wresp.Action)
		}

		if len(changedKey) == 0 {
			continue
		}

		tg, err := ed.makeTargetGroupForChangedKey(changedKey)
		if err != nil {
			return fmt.Errorf("error parse payload: %s", err.Error())
		}

		ch <- []*config.TargetGroup{tg}
	}
}

func (ed *EtcdDiscovery) fetchNodes(ctx context.Context) (uint64, error) {
	t0 := time.Now()
	resp, err := ed.kapi.Get(ctx, ed.keyPrefix, &etcdCli.GetOptions{
		Recursive: true,
	})
	etcdDuration.WithLabelValues("get").Observe(time.Since(t0).Seconds())

	if err != nil {
		etcdFailuresCount.WithLabelValues("get").Inc()
		return 0, fmt.Errorf("get key %q endpoints: %v error: %s",
			ed.keyPrefix, ed.client.Endpoints(), err.Error())
	}

	// init nodes each time we fetch nodes from etcd
	ed.nodes = make(map[string]string)

	ed.getAllNodes(resp.Node)
	return resp.Index, nil
}

func (ed *EtcdDiscovery) getAllNodes(rootNode *etcdCli.Node) {
	for _, node := range rootNode.Nodes {
		if node.Dir {
			ed.getAllNodes(node)
			continue
		}

		if strings.Contains(node.Key, ed.metricKey) {
			ed.nodes[node.Key] = node.Value
		}
	}
}

func (ed *EtcdDiscovery) makeTargetGroupsForAllNodes() ([]*config.TargetGroup, error) {
	tgByKey := make(map[string]*config.TargetGroup)
	for key, value := range ed.nodes {
		tgKey := ed.makeTargetGroupKey(key)
		tg, exists := tgByKey[tgKey]
		if !exists {
			tg = &config.TargetGroup{
				Source: tgKey,
			}
			tgByKey[tgKey] = tg
		}

		var info MetricInfo
		if err := json.Unmarshal([]byte(value), &info); err != nil {
			return nil, err
		}

		if tg.Labels == nil {
			labels := make(model.LabelSet, 3)
			for _, tag := range info.Tags {
				labels[model.LabelName(tag.Key)] = model.LabelValue(tag.Value)
			}
			tg.Labels = labels
		}

		tg.Targets = append(tg.Targets, model.LabelSet{
			model.AddressLabel: model.LabelValue(info.URL),
		})
	}
	result := make([]*config.TargetGroup, 0, len(tgByKey))
	for _, tg := range tgByKey {
		result = append(result, tg)
	}
	return result, nil
}

func (ed *EtcdDiscovery) makeTargetGroupForChangedKey(changedKey string) (*config.TargetGroup, error) {
	changedTargetGroupKey := ed.makeTargetGroupKey(changedKey)
	tg := config.TargetGroup{
		Source: changedTargetGroupKey,
	}

	for key, value := range ed.nodes {
		tgKey := ed.makeTargetGroupKey(key)
		if tgKey != changedTargetGroupKey {
			continue
		}

		var info MetricInfo
		if err := json.Unmarshal([]byte(value), &info); err != nil {
			return nil, fmt.Errorf("error: %s value: %q", err, value)
		}

		if tg.Labels == nil {
			labels := make(model.LabelSet, 3)
			for _, tag := range info.Tags {
				labels[model.LabelName(tag.Key)] = model.LabelValue(tag.Value)
			}
			tg.Labels = labels
		}

		tg.Targets = append(tg.Targets, model.LabelSet{
			model.AddressLabel: model.LabelValue(info.URL),
		})
	}
	return &tg, nil
}

func (ed *EtcdDiscovery) makeTargetGroupKey(key string) string {
	pos := strings.LastIndex(key, ed.metricKey)
	if pos > 0 {
		return key[:pos]
	}
	return key
}
