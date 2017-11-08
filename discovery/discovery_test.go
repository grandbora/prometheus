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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	yaml "gopkg.in/yaml.v2"
)

func TestSingleTargetSetWithSingleProviderOnlySendsNewTargetGroups(t *testing.T) {

	testCases := []struct {
		title   string
		updates []update
	}{
		{
			title:   "No updates",
			updates: []update{},
		},
		{
			title: "Empty initials",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{},
					interval:     5,
				},
			},
		},
		{
			title: "Empty initials with a delay",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{},
					interval:     6000,
				},
			},
		},
		{
			title: "Initials only",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}, {Source: "initial2"}},
					interval:     0,
				},
			},
		},
		{
			title: "Initials only but after a delay",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}, {Source: "initial2"}},
					interval:     6000,
				},
			},
		},
		{
			title: "Initials followed by empty updates",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}, {Source: "initial2"}},
					interval:     0,
				},
				{
					targetGroups: []config.TargetGroup{},
					interval:     10,
				},
			},
		},
		{
			title: "Initials and new groups",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}, {Source: "initial2"}},
					interval:     0,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update1"}, {Source: "update2"}},
					interval:     10,
				},
			},
		},
		{
			title: "Initials and new groups after a delay",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}, {Source: "initial2"}},
					interval:     6000,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update1"}, {Source: "update2"}},
					interval:     10,
				},
			},
		},
		{
			title: "Next test case",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}},
					interval:     100,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update1"}},
					interval:     100,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update2"}},
					interval:     10,
				},
			},
		},
		{
			title: "Next test case",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}, {Source: "initial2"}},
					interval:     100,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update1"}, {Source: "update2"}, {Source: "update3"}, {Source: "update4"}, {Source: "update5"}, {Source: "update6"}},
					interval:     30,
				},
			},
		},
		{
			title: "Next test case",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}},
					interval:     10,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update1"}},
					interval:     25,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update2"}, {Source: "update3"}, {Source: "update4"}},
					interval:     10,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update5"}},
					interval:     0,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update6"}, {Source: "update7"}, {Source: "update8"}},
					interval:     70,
				},
			},
		},
		{
			title: "Empty update in between",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{{Source: "initial1"}, {Source: "initial2"}},
					interval:     30,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update1"}, {Source: "update2"}},
					interval:     30,
				},
				{
					targetGroups: []config.TargetGroup{},
					interval:     10,
				},
				{
					targetGroups: []config.TargetGroup{{Source: "update3"}, {Source: "update4"}, {Source: "update5"}, {Source: "update6"}},
					interval:     30,
				},
			},
		},
	}

	for i, testCase := range testCases {

		expectedGroups := make(map[string]struct{})
		for _, update := range testCase.updates {
			for _, target := range update.targetGroups {
				expectedGroups[target.Source] = struct{}{}
			}
		}

		finalize := make(chan bool)

		isFirstSyncCall := true
		var initialGroups []*config.TargetGroup
		var syncedGroups []*config.TargetGroup

		targetSet := NewTargetSet(&mockSyncer{
			sync: func(tgs []*config.TargetGroup) {
				syncedGroups = tgs

				if isFirstSyncCall {
					isFirstSyncCall = false
					initialGroups = tgs
				}

				if len(tgs) == len(expectedGroups) {
					// All the groups are sent, we can start asserting.
					finalize <- true
				}
			},
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tp := newMockTargetProvider(testCase.updates)
		targetProviders := map[string]TargetProvider{}
		targetProviders["testProvider"] = tp

		go targetSet.Run(ctx)
		targetSet.UpdateProviders(targetProviders)

		select {
		case <-time.After(20000 * time.Millisecond):
			t.Errorf("In test case %v[%v]: Test timed out after 20000 millisecond. All targets should be sent within the timeout", i, testCase.title)

		case <-finalize:

			if *tp.callCount != 1 {
				t.Errorf("In test case %v[%v]: TargetProvider Run should be called once only, was called %v times", i, testCase.title, *tp.callCount)
			}

			if len(testCase.updates) > 0 && testCase.updates[0].interval > 5000 {
				// If the initial set of targets never arrive or arrive after 5 seconds.
				// The first sync call should receive empty set of targets.
				if len(initialGroups) != 0 {
					t.Errorf("In test case %v[%v]: Expecting 0 initial target groups, received %v", i, testCase.title, len(initialGroups))
				}
			}

			if len(syncedGroups) != len(expectedGroups) {
				t.Errorf("In test case %v[%v]: Expecting %v target groups in total, received %v", i, testCase.title, len(expectedGroups), len(syncedGroups))
			}

			for _, tg := range syncedGroups {
				if _, ok := expectedGroups[tg.Source]; ok == false {
					t.Errorf("In test case %v[%v]: '%s' does not exist in expected target groups: %s", i, testCase.title, tg.Source, expectedGroups)
				} else {
					delete(expectedGroups, tg.Source) // Remove used targets from the map.
				}
			}
		}
	}
}

func TestSingleTargetSetWithSingleProviderSendsUpdatedTargetGroups(t *testing.T) {

	// Function to determine if the sync call received the latest state of
	// all the target groups that came out of the target provider.
	endStateAchieved := func(groupsSentToSyc []*config.TargetGroup, endState map[string]config.TargetGroup) bool {

		if len(groupsSentToSyc) != len(endState) {
			return false
		}

		for _, tg := range groupsSentToSyc {
			if _, ok := endState[tg.Source]; ok == false {
				// The target group does not exist in the end state.
				return false
			}

			if reflect.DeepEqual(endState[tg.Source], *tg) == false {
				// The target group has not reached its final state yet.
				return false
			}

			delete(endState, tg.Source) // Remove used target groups.
		}

		return true
	}

	testCases := []struct {
		title   string
		updates []update
	}{
		{
			title: "Update same initial group multiple times",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "initial1",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.11:6003"}},
						},
					},
					interval: 3,
				},
			},
		},
		{
			title: "Update second round target",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "initial1",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.11:6003"}},
						},
					},
					interval: 3,
				},
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "update1",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.12:6003"}},
						},
					},
					interval: 10,
				},
				{
					targetGroups: []config.TargetGroup{
						{
							Source: "update1",
							Targets: []model.LabelSet{
								{"__instance__": "10.11.122.12:6003"},
								{"__instance__": "10.11.122.13:6003"},
								{"__instance__": "10.11.122.14:6003"},
							},
						},
					},
					interval: 10,
				},
			},
		},
		{
			title: "Next test case",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "initial1",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.11:6003"}},
						},
						{
							Source:  "initial2",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.12:6003"}},
						},
					},
					interval: 10,
				},
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "update1",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.13:6003"}},
						},
						{
							Source: "initial2",
							Targets: []model.LabelSet{
								{"__instance__": "10.11.122.12:6003"},
								{"__instance__": "10.11.122.14:6003"},
							},
						},
					},
					interval: 10,
				},
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "update2",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.15:6003"}},
						},
						{
							Source:  "initial1",
							Targets: []model.LabelSet{},
						},
					},
					interval: 10,
				},
				{
					targetGroups: []config.TargetGroup{
						{
							Source: "update1",
							Targets: []model.LabelSet{
								{"__instance__": "10.11.122.16:6003"},
								{"__instance__": "10.11.122.17:6003"},
								{"__instance__": "10.11.122.18:6003"},
							},
						},
					},
					interval: 10,
				},
			},
		},
		{
			title: "Next test case",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{
						{
							Source: "initial1",
							Targets: []model.LabelSet{
								{"__instance__": "10.11.122.11:6003"},
								{"__instance__": "10.11.122.12:6003"},
								{"__instance__": "10.11.122.13:6003"},
							},
						},
						{
							Source:  "initial2",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.14:6003"}},
						},
					},
					interval: 0,
				},
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "initial1",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.13:6003"}},
						},
					},
					interval: 10,
				},
			},
		},
		{
			title: "Three times sync call",
			updates: []update{
				{
					targetGroups: []config.TargetGroup{
						{
							Source: "initial1",
							Targets: []model.LabelSet{
								{"__instance__": "10.11.122.11:6003"},
								{"__instance__": "10.11.122.12:6003"},
								{"__instance__": "10.11.122.13:6003"},
							},
						},
						{
							Source:  "initial2",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.14:6003"}},
						},
					},
					interval: 1000,
				},
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "initial1",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.12:6003"}},
						},
						{
							Source:  "update1",
							Targets: []model.LabelSet{{"__instance__": "10.11.122.15:6003"}},
						},
					},
					interval: 3000,
				},
				{
					targetGroups: []config.TargetGroup{
						{
							Source:  "initial1",
							Targets: []model.LabelSet{},
						},
						{
							Source: "update1",
							Targets: []model.LabelSet{
								{"__instance__": "10.11.122.15:6003"},
								{"__instance__": "10.11.122.16:6003"},
							},
						},
					},
					interval: 3000,
				},
			},
		},
	}

	for i, testCase := range testCases {

		expectedGroups := make(map[string]config.TargetGroup)
		for _, update := range testCase.updates {
			for _, targetGroup := range update.targetGroups {
				expectedGroups[targetGroup.Source] = targetGroup
			}
		}

		finalize := make(chan bool)

		targetSet := NewTargetSet(&mockSyncer{
			sync: func(tgs []*config.TargetGroup) {

				endState := make(map[string]config.TargetGroup)
				for k, v := range expectedGroups {
					endState[k] = v
				}

				if endStateAchieved(tgs, endState) == false {
					return
				}

				finalize <- true
			},
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tp := newMockTargetProvider(testCase.updates)
		targetProviders := map[string]TargetProvider{}
		targetProviders["testProvider"] = tp

		go targetSet.Run(ctx)
		targetSet.UpdateProviders(targetProviders)

		select {
		case <-time.After(20000 * time.Millisecond):
			t.Errorf("In test case %v[%v]: Test timed out after 20000 millisecond. All targets should be sent within the timeout", i, testCase.title)

		case <-finalize:
			// System successfully reached to the end state.
		}
	}
}

func TestTargetSetRecreatesTargetGroupsEveryRun(t *testing.T) {

	verifyPresence := func(tgroups map[string]*config.TargetGroup, name string, present bool) {
		if _, ok := tgroups[name]; ok != present {
			msg := ""
			if !present {
				msg = "not "
			}
			t.Fatalf("'%s' should %sbe present in TargetSet.tgroups: %s", name, msg, tgroups)
		}
	}

	cfg := &config.ServiceDiscoveryConfig{}

	sOne := `
static_configs:
- targets: ["foo:9090"]
- targets: ["bar:9090"]
`
	if err := yaml.Unmarshal([]byte(sOne), cfg); err != nil {
		t.Fatalf("Unable to load YAML config sOne: %s", err)
	}
	called := make(chan struct{})

	ts := NewTargetSet(&mockSyncer{
		sync: func([]*config.TargetGroup) { called <- struct{}{} },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ts.Run(ctx)

	ts.UpdateProviders(ProvidersFromConfig(*cfg, nil))
	<-called

	verifyPresence(ts.tgroups, "static/0/0", true)
	verifyPresence(ts.tgroups, "static/0/1", true)

	sTwo := `
static_configs:
- targets: ["foo:9090"]
`
	if err := yaml.Unmarshal([]byte(sTwo), cfg); err != nil {
		t.Fatalf("Unable to load YAML config sTwo: %s", err)
	}

	ts.UpdateProviders(ProvidersFromConfig(*cfg, nil))
	<-called

	verifyPresence(ts.tgroups, "static/0/0", true)
	verifyPresence(ts.tgroups, "static/0/1", false)
}

func TestTargetSetRunsSameTargetProviderMultipleTimes(t *testing.T) {

	var wg sync.WaitGroup

	wg.Add(2)

	ts1 := NewTargetSet(&mockSyncer{
		sync: func([]*config.TargetGroup) { wg.Done() },
	})

	ts2 := NewTargetSet(&mockSyncer{
		sync: func([]*config.TargetGroup) { wg.Done() },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updates := []update{
		{
			targetGroups: []config.TargetGroup{{Source: "initial1"}, {Source: "initial2"}},
			interval:     10,
		},
	}

	tp := newMockTargetProvider(updates)
	targetProviders := map[string]TargetProvider{}
	targetProviders["testProvider"] = tp

	go ts1.Run(ctx)
	go ts2.Run(ctx)

	ts1.UpdateProviders(targetProviders)
	ts2.UpdateProviders(targetProviders)

	finalize := make(chan struct{})
	go func() {
		defer close(finalize)
		wg.Wait()
	}()

	select {
	case <-time.After(20000 * time.Millisecond):
		t.Error("Test timed out after 20000 millisecond. All targets should be sent within the timeout")

	case <-finalize:
		if *tp.callCount != 2 {
			t.Errorf("Was expecting 2 calls received %v", tp.callCount)
		}
	}
}

type mockSyncer struct {
	sync func(tgs []*config.TargetGroup)
}

func (s *mockSyncer) Sync(tgs []*config.TargetGroup) {
	if s.sync != nil {
		s.sync(tgs)
	}
}

type update struct {
	targetGroups []config.TargetGroup
	interval     time.Duration
}

type mockTargetProvider struct {
	callCount *uint32
	updates   []update
	up        chan<- []*config.TargetGroup
}

func newMockTargetProvider(updates []update) mockTargetProvider {
	var callCount uint32

	tp := mockTargetProvider{
		callCount: &callCount,
		updates:   updates,
	}

	return tp
}

func (tp mockTargetProvider) Run(ctx context.Context, up chan<- []*config.TargetGroup) {
	atomic.AddUint32(tp.callCount, 1)
	tp.up = up
	tp.sendUpdates()
}

func (tp mockTargetProvider) sendUpdates() {
	for _, update := range tp.updates {

		time.Sleep(update.interval * time.Millisecond)

		tgs := make([]*config.TargetGroup, len(update.targetGroups))
		for groupIndex := range update.targetGroups {

			tgs[groupIndex] = &update.targetGroups[groupIndex]
		}

		tp.up <- tgs
	}
}
