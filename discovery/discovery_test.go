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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/config"
	yaml "gopkg.in/yaml.v2"
)

type mockSyncer struct {
	sync func(tgs []*config.TargetGroup)
}

func (s *mockSyncer) Sync(tgs []*config.TargetGroup) {
	if s.sync != nil {
		s.sync(tgs)
	}
}

type mockTargetProvider struct {
	callCount *uint32
	updates   []update
	up        chan<- []*config.TargetGroup
}

type update struct {
	targets   []string
	interval time.Duration
}

func newMockTargetProvider(updates []update) mockTargetProvider {
	var callCount uint32

	tp := mockTargetProvider{
		callCount: &callCount,
		updates: updates,
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

		tgs := make([]*config.TargetGroup, len(update.targets))
		for i, tg := range update.targets {
			tgs[i] = &config.TargetGroup{Source: tg}
		}
		
		tp.up <- tgs
	}
}

func TestSingleTargetSetWithSingleProvider(t *testing.T) {

	testCases := []struct {
		updates []update
		expectedGroups map[string]struct{}
		isInitialDelayed bool
		maxSyncCallCount int
	}{
		// Provider sends initials only
		{
			updates: []update{{[]string{"initial1", "initial2"}, 0}},
			expectedGroups: map[string]struct{}{"initial1":struct{}{}, "initial2":struct{}{}},
			isInitialDelayed: false,
			maxSyncCallCount: 1,
		},
		// Provider sends initials only but after a delay
		{
			updates: []update{{[]string{"initial1", "initial2"}, 6000}},
			expectedGroups: map[string]struct{}{"initial1":struct{}{}, "initial2":struct{}{}},
			isInitialDelayed: true,
			maxSyncCallCount: 2,
		},
		{
			updates: []update{
	  		{[]string{"initial1", "initial2"}, 6000},
	  		{[]string{"update1", "update2"}, 500},
	  	},
			expectedGroups: map[string]struct{}{"initial1":struct{}{}, "initial2":struct{}{}, "update1":struct{}{}, "update2":struct{}{}},
			isInitialDelayed: true,
			maxSyncCallCount: 3,
		},
	}

	for i, testCase := range testCases {

		var wg sync.WaitGroup
		wg.Add(1)

		syncCallCount := 0
		var initialGroups []*config.TargetGroup
		var syncedGroups []*config.TargetGroup

		targetSet := NewTargetSet(&mockSyncer{
			sync: func(tgs []*config.TargetGroup){
				syncCallCount++
				syncedGroups = tgs

				if syncCallCount == 1 {
					initialGroups = tgs
				}

				if len(tgs) == len(testCase.expectedGroups) {
					// All the groups are sent, we can start asserting
					wg.Done()
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

		finalize := make(chan struct{})
		go func() {
			defer close(finalize)
			wg.Wait()
		}()

		select {
		case <-time.After(20000  * time.Millisecond):
			t.Errorf("In test case %v: Test timed out after 20000 millisecond. All targets should be sent within the timeout", i)

		case <-finalize:

			if *tp.callCount != 1 {
				t.Errorf("In test case %v: TargetProvider Run should be called once only, was called %v times", *tp.callCount, i)
			}

			if testCase.isInitialDelayed {
				// The initial target groups should be empty because provider didn't respond within 5 secs
				if len(initialGroups) != 0 {
					t.Errorf("In test case %v: Expecting 0 initial target groups, received %v", len(initialGroups), i)
				}
			}

			if syncCallCount > testCase.maxSyncCallCount {
				t.Errorf("In test case %v: Sync should be called at most %v times", testCase.maxSyncCallCount, i)
			}

			if len(syncedGroups) != len(testCase.expectedGroups) {
				t.Errorf("In test case %v: Expecting %v target groups, received %v", len(testCase.expectedGroups), len(syncedGroups), i)
			}

			for _, tg := range syncedGroups {
				if _, ok := testCase.expectedGroups[tg.Source]; ok == false {
					t.Errorf("In test case %v: '%s' does not exist in expected target groups: %s", tg.Source, testCase.expectedGroups, i)
				} else {
					delete(testCase.expectedGroups, tg.Source) // removed used targets from the map
				}
			}
		}
  }
}

func TestOneScrapeConfigWithOneProviderSendsInitialsAndUpdates(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	syncCallCount := 0
	var syncedGroups []*config.TargetGroup

	targetSet := NewTargetSet(&mockSyncer{
		sync: func(tgs []*config.TargetGroup){
			syncCallCount++
			syncedGroups = tgs

			if len(tgs) == 3 {
				// we are expecting 3 target groups
				wg.Done()
			}				
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
  
  tp := newMockTargetProvider([]update{
  	{[]string{"initial1"}, 0},
  	{[]string{"update1", "update2"}, 0},
  	})
	targetProviders := map[string]TargetProvider{}
	targetProviders["testProvider"] = tp

	go targetSet.Run(ctx)
	targetSet.UpdateProviders(targetProviders)

	finalize := make(chan struct{})
	go func() {
		defer close(finalize)
		wg.Wait()
	}()

	select {
	case <-time.After(20000  * time.Millisecond):
		t.Error("Test timed out after 20000 millisecond. All targets should be sent within the timeout")

	case <-finalize:

		if *tp.callCount != 1 {
			t.Errorf("TargetProvider Run should be called once only, was called %v times", *tp.callCount)
		}

		if syncCallCount > 2 {
			// At most 2 sync calls are expected because there is only one update after initials
			t.Fatalf("Sync should be called at most twice")
		}

		if len(syncedGroups) != 3 {
			t.Errorf("Expecting 3 target groups, received %v", len(syncedGroups))
		}

		expectedGroups := map[string]struct{}{"initial1":struct{}{}, "update1":struct{}{}, "update2":struct{}{}}
		for _, tg := range syncedGroups {
			if _, ok := expectedGroups[tg.Source]; ok == false {
				t.Errorf("'%s' does not exist in expected target groups: %s", tg.Source, expectedGroups)
			} else {
				delete(expectedGroups, tg.Source) // removed used targets from the map
			}
		}
	}
}



func TestOneScrapeConfigWithOneProviderSendsDelayedInitialsAndUpdates(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	syncCallCount := 0
	var syncedGroups []*config.TargetGroup

	targetSet := NewTargetSet(&mockSyncer{
		sync: func(tgs []*config.TargetGroup){
			syncCallCount++
			syncedGroups = tgs

			if len(tgs) == 4 {
				// We are expecting 4 target groups
				wg.Done()
			}				
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
  
  tp := newMockTargetProvider([]update{
  	{[]string{"initial1", "initial2"}, 6000},
  	{[]string{"update1", "update2"}, 500},
  	})
	targetProviders := map[string]TargetProvider{}
	targetProviders["testProvider"] = tp

	go targetSet.Run(ctx)
	targetSet.UpdateProviders(targetProviders)

	finalize := make(chan struct{})
	go func() {
		defer close(finalize)
		wg.Wait()
	}()

	select {
	case <-time.After(20000  * time.Millisecond):
		t.Error("Test timed out after 20000 millisecond. All targets should be sent within the timeout")

	case <-finalize:

		if *tp.callCount != 1 {
			t.Errorf("TargetProvider Run should be called once only, was called %v times", *tp.callCount)
		}

		if syncCallCount > 3 {
			t.Fatalf("Sync should be called at most 3 times")
		}

		if len(syncedGroups) != 4 {
			t.Errorf("Expecting 4 target groups, received %v", len(syncedGroups))
		}

		expectedGroups := map[string]struct{}{"initial1":struct{}{}, "initial2":struct{}{}, "update1":struct{}{}, "update2":struct{}{}}
		for _, tg := range syncedGroups {
			if _, ok := expectedGroups[tg.Source]; ok == false {
				t.Errorf("'%s' does not exist in expected target groups: %s", tg.Source, expectedGroups)
			} else {
				delete(expectedGroups, tg.Source) // removed used targets from the map
			}
		}
	}
}

func TestTargetSetRecreatesTargetGroupsEveryRun(t *testing.T) {

	t.Skip("laterz")

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
		// sync: func([]*config.TargetGroup) { called <- struct{}{} },
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
	t.Skip("laterz")

	var wg sync.WaitGroup

	wg.Add(2)

	ts1 := NewTargetSet(&mockSyncer{
		// sync: func([]*config.TargetGroup) { wg.Done() },
	})

	ts2 := NewTargetSet(&mockSyncer{
		// sync: func([]*config.TargetGroup) { wg.Done() },
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := mockTargetProvider{}
	var callCount uint32
	tp.callCount = &callCount

	targetProviders := map[string]TargetProvider{}
	targetProviders["testProvider"] = tp

	go ts1.Run(ctx)
	go ts2.Run(ctx)

	ts1.UpdateProviders(targetProviders)
	ts2.UpdateProviders(targetProviders)
	wg.Wait()

	if callCount != 2 {
		t.Errorf("Was expecting 2 calls received %v", callCount)
	}
}
