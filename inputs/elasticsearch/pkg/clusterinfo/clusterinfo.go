// Copyright 2021 The Prometheus Authors
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

package clusterinfo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "elasticsearch"
	subsystem = "clusterinfo"
)

var (
	// ErrConsumerAlreadyRegistered is returned if a consumer is already registered
	ErrConsumerAlreadyRegistered = errors.New("consumer already registered")
	// ErrInitialCallTimeout is returned if the initial clusterinfo call timed out
	ErrInitialCallTimeout = errors.New("initial cluster info call timed out")
	// ErrRetrieverStopped is returned when trying to register a consumer after retriever is stopped
	ErrRetrieverStopped = errors.New("retriever is stopped")
	initialTimeout      = 10 * time.Second
)

type consumer interface {
	// ClusterLabelUpdates returns a pointer to channel for cluster label updates
	ClusterLabelUpdates() *chan *Response
	// String implements the stringer interface
	String() string
}

// Retriever periodically gets the cluster info from the / endpoint and
// sends it to all registered consumer channels
type Retriever struct {
	consumerChannels      map[string]*chan *Response
	client                *http.Client
	url                   *url.URL
	interval              time.Duration
	sync                  chan struct{}
	mu                    sync.RWMutex // 保护以下字段
	stopped               bool         // 标记Retriever是否已停止
	versionMetric         *prometheus.GaugeVec
	up                    *prometheus.GaugeVec
	lastUpstreamSuccessTs *prometheus.GaugeVec
	lastUpstreamErrorTs   *prometheus.GaugeVec
}

// New creates a new Retriever
func New(client *http.Client, u *url.URL, interval time.Duration) *Retriever {
	return &Retriever{
		consumerChannels: make(map[string]*chan *Response),
		client:           client,
		url:              u,
		interval:         interval,
		sync:             make(chan struct{}, 1),
		mu:               sync.RWMutex{},
		stopped:          false,
		versionMetric: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prometheus.BuildFQName(namespace, subsystem, "version_info"),
				Help: "Constant metric with ES version information as labels",
			},
			[]string{
				"cluster",
				"cluster_uuid",
				"build_date",
				"build_hash",
				"version",
				"lucene_version",
			},
		),
		up: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prometheus.BuildFQName(namespace, subsystem, "up"),
				Help: "Up metric for the cluster info collector",
			},
			[]string{"url"},
		),
		lastUpstreamSuccessTs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prometheus.BuildFQName(namespace, subsystem, "last_retrieval_success_ts"),
				Help: "Timestamp of the last successful cluster info retrieval",
			},
			[]string{"url"},
		),
		lastUpstreamErrorTs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prometheus.BuildFQName(namespace, subsystem, "last_retrieval_failure_ts"),
				Help: "Timestamp of the last failed cluster info retrieval",
			},
			[]string{"url"},
		),
	}
}

// Describe implements the prometheus.Collector interface
func (r *Retriever) Describe(ch chan<- *prometheus.Desc) {
	r.versionMetric.Describe(ch)
	r.up.Describe(ch)
	r.lastUpstreamSuccessTs.Describe(ch)
	r.lastUpstreamErrorTs.Describe(ch)
}

// Collect implements the prometheus.Collector interface
func (r *Retriever) Collect(ch chan<- prometheus.Metric) {
	r.versionMetric.Collect(ch)
	r.up.Collect(ch)
	r.lastUpstreamSuccessTs.Collect(ch)
	r.lastUpstreamErrorTs.Collect(ch)
}

func (r *Retriever) updateMetrics(res *Response) {
	u := *r.url
	u.User = nil
	url := u.String()
	// scrape failed, response is nil
	if res == nil {
		r.up.WithLabelValues(url).Set(0.0)
		r.lastUpstreamErrorTs.WithLabelValues(url).Set(float64(time.Now().Unix()))
		return
	}
	r.up.WithLabelValues(url).Set(1.0)
	r.versionMetric.WithLabelValues(
		res.ClusterName,
		res.ClusterUUID,
		res.Version.BuildDate,
		res.Version.BuildHash,
		res.Version.Number.String(),
		res.Version.LuceneVersion.String(),
	).Set(1.0)
	r.lastUpstreamSuccessTs.WithLabelValues(url).Set(float64(time.Now().Unix()))
}

// Update triggers an external cluster info label update
func (r *Retriever) Update() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.stopped {
		return
	}
	select {
	case r.sync <- struct{}{}:
	default:
		// sync channel is full, skip this update
	}
}

// RegisterConsumer registers a consumer for cluster info updates
func (r *Retriever) RegisterConsumer(c consumer) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		return ErrRetrieverStopped
	}

	if _, registered := r.consumerChannels[c.String()]; registered {
		return ErrConsumerAlreadyRegistered
	}
	r.consumerChannels[c.String()] = c.ClusterLabelUpdates()
	return nil
}

// UnregisterConsumer removes a consumer from receiving updates
func (r *Retriever) UnregisterConsumer(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.consumerChannels, name)
}

// safeSend 安全地向channel发送数据，如果channel已关闭则从注册表中移除该消费者
func (r *Retriever) safeSend(consumerCh *chan *Response, name string, res *Response) bool {
	if consumerCh == nil {
		return false
	}

	// 使用recover捕获向已关闭channel发送数据导致的panic
	defer func() {
		if panicErr := recover(); panicErr != nil {
			log.Printf("WARN: panic recovered when sending to consumer %s: %v\n", name, panicErr)
			// 从注册表中移除该消费者，因为它的channel已关闭
			r.mu.Lock()
			delete(r.consumerChannels, name)
			r.mu.Unlock()
		}
	}()

	// 尝试发送数据
	select {
	case *consumerCh <- res:
		log.Println("successfully sent update to consumer:", name)
		return true
	default:
		// channel已满，跳过此消费者
		log.Println("channel for consumer", name, "is full, skipping update")
		return false
	}
}

// Run starts the update loop and periodically queries the / endpoint
// The update loop is terminated upon ctx cancellation. The call blocks until the first
// call to the cluster info endpoint was successful
func (r *Retriever) Run(ctx context.Context) error {
	startupComplete := make(chan struct{})

	// start update routine
	go func(ctx context.Context) {
		for {
			// 检查是否已停止
			r.mu.RLock()
			stopped := r.stopped
			r.mu.RUnlock()

			if stopped {
				return
			}

			select {
			case <-ctx.Done():
				r.mu.Lock()
				r.stopped = true
				// 安全关闭所有消费者频道
				for name, ch := range r.consumerChannels {
					if ch != nil {
						log.Println("closing channel for consumer:", name)
						// 注意：我们不应该关闭消费者提供的channel
						// 这里只是记录，不实际关闭
					}
				}
				// 清空map，防止内存泄漏
				r.consumerChannels = make(map[string]*chan *Response)
				r.mu.Unlock()
				log.Println("context cancelled, exiting cluster info update loop")
				return

			case <-r.sync:
				// 再次检查是否已停止
				r.mu.RLock()
				if r.stopped {
					r.mu.RUnlock()
					return
				}
				// 创建消费者频道的快照，避免发送过程中map被修改
				consumers := make(map[string]*chan *Response)
				for k, v := range r.consumerChannels {
					consumers[k] = v
				}
				r.mu.RUnlock()

				res, err := r.fetchAndDecodeClusterInfo()
				if err != nil {
					log.Println("failed to retrieve cluster info from ES, err:", err)
					r.updateMetrics(nil)
					continue
				}

				r.updateMetrics(res)

				// 安全地向所有消费者发送更新
				for name, consumerCh := range consumers {
					r.safeSend(consumerCh, name, res)
				}

				// 关闭startupComplete如果尚未关闭
				select {
				case <-startupComplete:
				default:
					close(startupComplete)
				}
			}
		}
	}(ctx)

	// trigger initial cluster info call
	log.Println("triggering initial cluster info call")
	r.Update()

	// start a ticker routine
	go func(ctx context.Context) {
		if r.interval <= 0 {
			log.Println("no periodic cluster info label update requested")
			return
		}

		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			// 检查是否已停止
			r.mu.RLock()
			stopped := r.stopped
			r.mu.RUnlock()

			if stopped {
				return
			}

			select {
			case <-ctx.Done():
				log.Println("context cancelled, exiting cluster info trigger loop")
				return
			case <-ticker.C:
				// 再次检查是否已停止
				r.mu.RLock()
				if r.stopped {
					r.mu.RUnlock()
					return
				}
				r.mu.RUnlock()

				log.Println("triggering periodic update")
				r.Update()
			}
		}
	}(ctx)

	// block until the first retrieval was successful
	select {
	case <-startupComplete:
		log.Println("initial clusterinfo sync succeeded")
		return nil
	case <-time.After(initialTimeout):
		return ErrInitialCallTimeout
	case <-ctx.Done():
		return nil
	}
}

func (r *Retriever) fetchAndDecodeClusterInfo() (*Response, error) {
	var response *Response
	u := *r.url
	u.Path = path.Join(r.url.Path, "/")

	res, err := r.client.Get(u.String())
	if err != nil {
		log.Println("failed to get cluster info, err:", err)
		return nil, err
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			log.Println("failed to close http.Client, err:", err)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP Request failed with code %d", res.StatusCode)
	}

	bts, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(bts, &response); err != nil {
		return nil, err
	}

	return response, nil
}

// Stop gracefully stops the retriever
func (r *Retriever) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stopped {
		return
	}

	r.stopped = true
	// 注意：我们不关闭消费者提供的channel，只清空注册表
	r.consumerChannels = make(map[string]*chan *Response)
}
