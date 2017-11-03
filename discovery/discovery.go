// Copyright 2016 The Prometheus Authors
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

package discovery

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/azure"
	"github.com/prometheus/prometheus/discovery/consul"
	"github.com/prometheus/prometheus/discovery/dns"
	"github.com/prometheus/prometheus/discovery/ec2"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/gce"
	"github.com/prometheus/prometheus/discovery/kubernetes"
	"github.com/prometheus/prometheus/discovery/marathon"
	"github.com/prometheus/prometheus/discovery/openstack"
	"github.com/prometheus/prometheus/discovery/triton"
	"github.com/prometheus/prometheus/discovery/zookeeper"
)

// A TargetProvider provides information about target groups. It maintains a set
// of sources from which TargetGroups can originate. Whenever a target provider
// detects a potential change, it sends the TargetGroup through its provided channel.
//
// The TargetProvider does not have to guarantee that an actual change happened.
// It does guarantee that it sends the new TargetGroup whenever a change happens.
//
// TargetProviders should initially send a full set of all discoverable TargetGroups.
type TargetProvider interface {
	// Run hands a channel to the target provider through which it can send
	// updated target groups.
	// Must returns if the context gets canceled. It should not close the update
	// channel on returning.
	Run(ctx context.Context, up chan<- []*config.TargetGroup)
}

// ProvidersFromConfig returns all TargetProviders configured in cfg.
func ProvidersFromConfig(cfg config.ServiceDiscoveryConfig, logger log.Logger) map[string]TargetProvider {
	providers := map[string]TargetProvider{}

	app := func(mech string, i int, tp TargetProvider) {
		providers[fmt.Sprintf("%s/%d", mech, i)] = tp
	}

	for i, c := range cfg.DNSSDConfigs {
		app("dns", i, dns.NewDiscovery(c, log.With(logger, "discovery", "dns")))
	}
	for i, c := range cfg.FileSDConfigs {
		app("file", i, file.NewDiscovery(c, log.With(logger, "discovery", "file")))
	}
	for i, c := range cfg.ConsulSDConfigs {
		k, err := consul.NewDiscovery(c, log.With(logger, "discovery", "consul"))
		if err != nil {
			level.Error(logger).Log("msg", "Cannot create Consul discovery", "err", err)
			continue
		}
		app("consul", i, k)
	}
	for i, c := range cfg.MarathonSDConfigs {
		m, err := marathon.NewDiscovery(c, log.With(logger, "discovery", "marathon"))
		if err != nil {
			level.Error(logger).Log("msg", "Cannot create Marathon discovery", "err", err)
			continue
		}
		app("marathon", i, m)
	}
	for i, c := range cfg.KubernetesSDConfigs {
		k, err := kubernetes.New(log.With(logger, "discovery", "k8s"), c)
		if err != nil {
			level.Error(logger).Log("msg", "Cannot create Kubernetes discovery", "err", err)
			continue
		}
		app("kubernetes", i, k)
	}
	for i, c := range cfg.ServersetSDConfigs {
		app("serverset", i, zookeeper.NewServersetDiscovery(c, log.With(logger, "discovery", "zookeeper")))
	}
	for i, c := range cfg.NerveSDConfigs {
		app("nerve", i, zookeeper.NewNerveDiscovery(c, log.With(logger, "discovery", "nerve")))
	}
	for i, c := range cfg.EC2SDConfigs {
		app("ec2", i, ec2.NewDiscovery(c, log.With(logger, "discovery", "ec2")))
	}
	for i, c := range cfg.OpenstackSDConfigs {
		openstackd, err := openstack.NewDiscovery(c, log.With(logger, "discovery", "openstack"))
		if err != nil {
			level.Error(logger).Log("msg", "Cannot initialize OpenStack discovery", "err", err)
			continue
		}
		app("openstack", i, openstackd)
	}

	for i, c := range cfg.GCESDConfigs {
		gced, err := gce.NewDiscovery(c, log.With(logger, "discovery", "gce"))
		if err != nil {
			level.Error(logger).Log("msg", "Cannot initialize GCE discovery", "err", err)
			continue
		}
		app("gce", i, gced)
	}
	for i, c := range cfg.AzureSDConfigs {
		app("azure", i, azure.NewDiscovery(c, log.With(logger, "discovery", "azure")))
	}
	for i, c := range cfg.TritonSDConfigs {
		t, err := triton.New(log.With(logger, "discovery", "trition"), c)
		if err != nil {
			level.Error(logger).Log("msg", "Cannot create Triton discovery", "err", err)
			continue
		}
		app("triton", i, t)
	}
	if len(cfg.StaticConfigs) > 0 {
		app("static", 0, NewStaticProvider(cfg.StaticConfigs))
	}

	return providers
}

// StaticProvider holds a list of target groups that never change.
type StaticProvider struct {
	TargetGroups []*config.TargetGroup
}

// NewStaticProvider returns a StaticProvider configured with the given
// target groups.
func NewStaticProvider(groups []*config.TargetGroup) *StaticProvider {
	for i, tg := range groups {
		tg.Source = fmt.Sprintf("%d", i)
	}
	return &StaticProvider{groups}
}

// Run implements the TargetProvider interface.
func (sd *StaticProvider) Run(ctx context.Context, ch chan<- []*config.TargetGroup) {
	// We still have to consider that the consumer exits right away in which case
	// the context will be canceled.
	select {
	case ch <- sd.TargetGroups:
	case <-ctx.Done():
	}
	close(ch)
}

var mu = &sync.Mutex{}
var targetProviders = make(map[*TargetProvider][]*TargetSet)
var currentGroups = make(map[*TargetProvider][]*config.TargetGroup)

// TargetSet handles multiple TargetProviders and sends a full overview of their
// discovered TargetGroups to a Syncer.
type TargetSet struct {
	mtx sync.RWMutex
	// Sets of targets by a source string that is unique across target providers.
	tgroups map[string]*config.TargetGroup

	syncer Syncer

	syncCh          chan struct{}
	providerCh      chan map[string]TargetProvider
	cancelProviders func()
}

// Syncer receives updates complete sets of TargetGroups.
type Syncer interface {
	Sync([]*config.TargetGroup)
}

// NewTargetSet returns a new target sending TargetGroups to the Syncer.
func NewTargetSet(s Syncer) *TargetSet {
	return &TargetSet{
		syncCh:     make(chan struct{}, 1),
		providerCh: make(chan map[string]TargetProvider),
		syncer:     s,
	}
}

// Run starts the processing of target providers and their updates.
// It blocks until the context gets canceled.
func (ts *TargetSet) Run(ctx context.Context) {
Loop:
	for {
		// Throttle syncing to once per five seconds.
		select {
		case <-ctx.Done():
			break Loop
		case p := <-ts.providerCh:
			ts.updateProviders(ctx, p)
		case <-time.After(5 * time.Second):
		}

		select {
		case <-ctx.Done():
			break Loop
		case <-ts.syncCh:
			ts.sync()
		case p := <-ts.providerCh:
			ts.updateProviders(ctx, p)
		}
	}
}

func (ts *TargetSet) sync() {
	ts.mtx.RLock()
	var all []*config.TargetGroup
	for _, tg := range ts.tgroups {
		all = append(all, tg)
	}
	ts.mtx.RUnlock()

	ts.syncer.Sync(all)
}

// UpdateProviders sets new target providers for the target set.
func (ts *TargetSet) UpdateProviders(p map[string]TargetProvider) {
	ts.providerCh <- p
}

// TODO:
// implement cancelling the old target providers
func (ts *TargetSet) updateProviders(ctx context.Context, providers map[string]TargetProvider) {

	var wg sync.WaitGroup

	// (Re-)create a fresh tgroups map to not keep stale targets around. We
	// will retrieve all targets below anyway, so cleaning up everything is
	// safe and doesn't inflict any additional cost.
	ts.mtx.Lock()
	ts.tgroups = map[string]*config.TargetGroup{}
	ts.mtx.Unlock()

	for newProviderName, newProvider := range providers {

		var preExistingProvider *TargetProvider = nil

		mu.Lock()

		for existingProvider, _ := range targetProviders {
			if reflect.DeepEqual(newProvider, existingProvider) {
				preExistingProvider = existingProvider
				break
			}
		}

		if preExistingProvider != nil {
			// Bind this target set to an existing provider

			// Set the latest state of the groups as initial if not empty
			currentGroups := currentGroups[preExistingProvider]
			if len(currentGroups) > 0 {

				for _, tgroup := range currentGroups {
					ts.setTargetGroup(newProviderName, tgroup)
				}
			}

			// Bind the target set to the future updates
			followers := targetProviders[preExistingProvider]
			followers = append(followers, ts)
			targetProviders[preExistingProvider] = followers

		} else {
			// Register a new target provider

			// Current groups of this provider is empty at the moment
			currentGroups[&newProvider] = make([]*config.TargetGroup, 0)

			// Set the followers of this provider
			targetSets := make([]*TargetSet, 0)
			targetSets = append(targetSets, ts)
			targetProviders[&newProvider] = targetSets
		}

		mu.Unlock()

		if preExistingProvider == nil {
			// Start running the newly added provider
			wg.Add(1)

			updates := make(chan []*config.TargetGroup)
			go newProvider.Run(ctx, updates)

			go func(name string, prov *TargetProvider) {
				select {
				case <-ctx.Done():
				case initial, ok := <-updates:
					// Handle the case that a target provider exits and closes the channel
					// before the context is done.
					if !ok {
						break
					}

					mu.Lock()

					// Update the current groups of the provider with the initial set of targets
					currentGroups[prov] = initial

					// Update all of the followers of this provider with the same initials
					for _, ts := range targetProviders[prov] {
						for _, tgroup := range initial {
							ts.setTargetGroup(name, tgroup)
						}
					}

					mu.Unlock()

				case <-time.After(5 * time.Second):
					// Initial set didn't arrive. Act as if it was empty
					// and wait for updates later on.
				}

				wg.Done()

				// Start listening for further updates.
				for {
					select {
					case <-ctx.Done():
						return
					case tgs, ok := <-updates:
						// Handle the case that a target provider exits and closes the channel
						// before the context is done.
						if !ok {
							return
						}

						mu.Lock()

						// Keep current groups up to date
						currentGroups[prov] = append(currentGroups[prov], tgs...)

						// Update all of the followers
						for _, ts := range targetProviders[prov] {
							for _, tg := range tgs {
								ts.update(name, tg)
							}
						}

						mu.Unlock()

					}
				}
			}(newProviderName, &newProvider)
		}
	}

	// We wait for a full initial set of target groups before releasing the mutex
	// to ensure the initial sync is complete and there are no races with subsequent updates.
	wg.Wait()
	// Just signal that there are initial sets to sync now. Actual syncing must only
	// happen in the runScraping loop.
	select {
	case ts.syncCh <- struct{}{}:
	default:
	}
}

// update handles a target group update from a target provider identified by the name.
func (ts *TargetSet) update(name string, tgroup *config.TargetGroup) {
	ts.setTargetGroup(name, tgroup)

	select {
	case ts.syncCh <- struct{}{}:
	default:
	}
}

func (ts *TargetSet) setTargetGroup(name string, tg *config.TargetGroup) {
	ts.mtx.Lock()
	defer ts.mtx.Unlock()

	if tg == nil {
		return
	}
	ts.tgroups[name+"/"+tg.Source] = tg
}
