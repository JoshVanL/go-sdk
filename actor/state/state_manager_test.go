/*
Copyright 2021 The Dapr Authors
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

package state

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Add(t *testing.T) {
	t.Parallel()

	const (
		tActorType = "test-state-name"
		tActorID   = "test-actor-id"
	)

	tests := map[string]struct {
		popTracker  func(*sync.Map)
		withAsync   func(*testing.T, *fakeAsync)
		stateName   string
		value       any
		expErr      bool
		postTracker func(*testing.T, sync.Map)
	}{
		"if state name is empty, expect error": {
			stateName: "",
			expErr:    true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				tracker.Range(func(key, value any) bool {
					t.Errorf("unexpected state change: %s", key)
					return true
				})
			},
		},
		"adding to an empty tracker and doesn't exist in provider should write object to tracker": {
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return false, nil
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    false,
			postTracker: func(t *testing.T, tracker sync.Map) {
				val, ok := tracker.Load("stateName1")
				assert.True(t, ok)
				assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value1"}, val)
				tracker.Range(func(key, value any) bool {
					assert.Equal(t, "stateName1", key)
					assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value1"}, value)
					return true
				})
			},
		},
		"adding to an empty tracker and but provider errors, should return error": {
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return false, errors.New("error")
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				tracker.Range(func(key, value any) bool {
					t.Errorf("unexpected state change: %s", key)
					return true
				})
			},
		},
		"adding to an empty tracker but provider errors, should return error": {
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return false, errors.New("error")
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				tracker.Range(func(key, value any) bool {
					t.Errorf("unexpected state change: %s", key)
					return true
				})
			},
		},
		"adding to an empty tracker but exists in provider, should return error": {
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return true, nil
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				tracker.Range(func(key, value any) bool {
					t.Errorf("unexpected state change: %s", key)
					return true
				})
			},
		},
		"adding to a tracker which doesn't have the same key or in provider, should write object to tracker": {
			popTracker: func(tracker *sync.Map) {
				tracker.Store("stateName2", &ChangeMetadata{Kind: Add, Value: "value2"})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return false, nil
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    false,
			postTracker: func(t *testing.T, tracker sync.Map) {
				val, ok := tracker.Load("stateName1")
				assert.True(t, ok)
				assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value1"}, val)
				var i int
				tracker.Range(func(key, value any) bool {
					assert.Contains(t, []string{"stateName1", "stateName2"}, key)
					assert.Contains(t, []*ChangeMetadata{
						&ChangeMetadata{Kind: Add, Value: "value1"},
						&ChangeMetadata{Kind: Add, Value: "value2"},
					}, value)
					i++
					return true
				})
				assert.Equal(t, 2, i)
			},
		},
		"adding to a tracker which already exists in tracker but not provider should error": {
			popTracker: func(tracker *sync.Map) {
				tracker.Store("stateName1", &ChangeMetadata{Kind: Add, Value: "value2"})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return false, nil
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				val, ok := tracker.Load("stateName1")
				assert.True(t, ok)
				assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value2"}, val)
				tracker.Range(func(key, value any) bool {
					assert.Equal(t, "stateName1", key)
					assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value2"}, value)
					return true
				})
			},
		},
		"adding to a tracker which already exists in tracker and provider should error": {
			popTracker: func(tracker *sync.Map) {
				tracker.Store("stateName1", &ChangeMetadata{Kind: Add, Value: "value2"})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return true, nil
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				val, ok := tracker.Load("stateName1")
				assert.True(t, ok)
				assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value2"}, val)
				tracker.Range(func(key, value any) bool {
					assert.Equal(t, "stateName1", key)
					assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value2"}, value)
					return true
				})
			},
		},
		"adding to a tracker which already exists in tracker but Removed, and is not provider should write object": {
			popTracker: func(tracker *sync.Map) {
				tracker.Store("stateName1", &ChangeMetadata{Kind: Remove, Value: "value2"})
				tracker.Store("stateName2", &ChangeMetadata{Kind: Remove, Value: "value3"})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return false, nil
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    false,
			postTracker: func(t *testing.T, tracker sync.Map) {
				val, ok := tracker.Load("stateName1")
				assert.True(t, ok)
				assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value1"}, val)
				var i int
				tracker.Range(func(key, value any) bool {
					assert.Contains(t, []string{"stateName1", "stateName2"}, key)
					assert.Contains(t, []*ChangeMetadata{
						&ChangeMetadata{Kind: Add, Value: "value1"},
						&ChangeMetadata{Kind: Remove, Value: "value3"},
					}, value)
					i++
					return true
				})
				assert.Equal(t, 2, i)
			},
		},
		"adding to a tracker which already exists in tracker but Removed, and is in provider should write object": {
			popTracker: func(tracker *sync.Map) {
				tracker.Store("stateName1", &ChangeMetadata{Kind: Remove, Value: "value2"})
				tracker.Store("stateName2", &ChangeMetadata{Kind: Remove, Value: "value3"})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnContainsContext(func(_ context.Context, actType, actID, stateName string) (bool, error) {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return true, nil
				})
			},
			stateName: "stateName1",
			value:     "value1",
			expErr:    false,
			postTracker: func(t *testing.T, tracker sync.Map) {
				val, ok := tracker.Load("stateName1")
				assert.True(t, ok)
				assert.Equal(t, &ChangeMetadata{Kind: Add, Value: "value1"}, val)
				var i int
				tracker.Range(func(key, value any) bool {
					assert.Contains(t, []string{"stateName1", "stateName2"}, key)
					assert.Contains(t, []*ChangeMetadata{
						&ChangeMetadata{Kind: Add, Value: "value1"},
						&ChangeMetadata{Kind: Remove, Value: "value3"},
					}, value)
					i++
					return true
				})
				assert.Equal(t, 2, i)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			prov := newFakeAsync(t)
			if test.withAsync != nil {
				test.withAsync(t, prov)
			}

			sm := NewActorStateManagerContext(tActorType, tActorID, nil).(*stateManagerCtx)
			sm.stateAsyncProvider = prov

			if test.popTracker != nil {
				test.popTracker(&sm.stateChangeTracker)
			}

			err := sm.Add(context.Background(), test.stateName, test.value)
			assert.Equal(t, test.expErr, err != nil, "unexpected error: %v", err)
			if test.postTracker != nil {
				test.postTracker(t, sm.stateChangeTracker)
			}
		})
	}
}

func Test_Get(t *testing.T) {
	t.Parallel()

	const (
		tActorType = "test-state-name"
		tActorID   = "test-actor-id"
	)

	tests := map[string]struct {
		popTracker  func(*sync.Map)
		withAsync   func(*testing.T, *fakeAsync)
		stateName   string
		expReply    int
		expErr      bool
		postTracker func(*testing.T, sync.Map)
	}{
		"if state name is empty, expect error": {
			stateName: "",
			expErr:    true,
		},
		"if key not in tracker, async returns error, expect reply nil": {
			stateName: "stateName1",
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnLoadContext(func(_ context.Context, actType, actID, stateName string, reply any) error {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return errors.New("an error")
				})
			},
			expReply: 0,
			expErr:   true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				tracker.Range(func(key, value any) bool {
					t.Errorf("unexpected key in tracker: %v", key)
					return true
				})
			},
		},
		"if key in state store but is Removed, expect error": {
			stateName: "stateName1",
			popTracker: func(tracker *sync.Map) {
				i := 1
				tracker.Store("stateName1", &ChangeMetadata{Kind: Remove, Value: &i})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnLoadContext(func(_ context.Context, actType, actID, stateName string, reply any) error {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return nil
				})
			},
			expReply: 0,
			expErr:   true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				var i int
				tracker.Range(func(key, value any) bool {
					assert.Equal(t, "stateName1", key)
					assert.Equal(t, &ChangeMetadata{Kind: Remove, Value: nil}, value)
					return true
				})
				assert.Equal(t, 1, i)
			},
		},
		"if key in state store but has been expired, async returns nothing, expect empty reply": {
			stateName: "stateName1",
			popTracker: func(tracker *sync.Map) {
				i := 1
				ttl := time.Second * 100
				tracker.Store("stateName1", &ChangeMetadata{Kind: Add, Value: &i, TTL: &ttl, estimateTTL: time.Now().Add(-time.Second)})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnLoadContext(func(_ context.Context, actType, actID, stateName string, reply any) error {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return nil
				})
			},
			expReply: 0,
			expErr:   false,
			postTracker: func(t *testing.T, tracker sync.Map) {
				var i int
				tracker.Range(func(key, value any) bool {
					assert.Equal(t, "stateName1", key)
					assert.Equal(t, &ChangeMetadata{Kind: None, Value: nil, TTL: nil}, value)
					return true
				})
				assert.Equal(t, 1, i)
			},
		},
		"if key in state store but has been expired, async returns error, expect empty reply and error": {
			stateName: "stateName1",
			popTracker: func(tracker *sync.Map) {
				i := 1
				ttl := time.Second * 100
				tracker.Store("stateName1", &ChangeMetadata{Kind: Add, Value: &i, TTL: &ttl, estimateTTL: time.Now().Add(-time.Second)})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnLoadContext(func(_ context.Context, actType, actID, stateName string, reply any) error {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return errors.New("an error")
				})
			},
			expReply: 0,
			expErr:   true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				var i int
				tracker.Range(func(key, value any) bool {
					assert.Equal(t, "stateName1", key)
					assert.Equal(t, &ChangeMetadata{Kind: None, Value: nil, TTL: nil}, value)
					return true
				})
				assert.Equal(t, 1, i)
			},
		},
		"if key in state store but has been expired, async returns value, expect reply and no error": {
			stateName: "stateName1",
			popTracker: func(tracker *sync.Map) {
				i := 1
				ttl := time.Second * 100
				tracker.Store("stateName1", &ChangeMetadata{Kind: Add, Value: &i, TTL: &ttl, estimateTTL: time.Now().Add(-time.Second)})
			},
			withAsync: func(t *testing.T, prov *fakeAsync) {
				var i int
				prov.WithFnLoadContext(func(_ context.Context, actType, actID, stateName string, reply any) error {
					assert.Equal(t, tActorType, actType)
					assert.Equal(t, tActorID, actID)
					assert.Equal(t, "stateName1", stateName)
					assert.Equal(t, 0, i)
					i++
					return errors.New("an error")
				})
			},
			expReply: 0,
			expErr:   true,
			postTracker: func(t *testing.T, tracker sync.Map) {
				var i int
				tracker.Range(func(key, value any) bool {
					assert.Equal(t, "stateName1", key)
					assert.Equal(t, &ChangeMetadata{Kind: None, Value: nil, TTL: nil}, value)
					return true
				})
				assert.Equal(t, 1, i)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			prov := newFakeAsync(t)
			if test.withAsync != nil {
				test.withAsync(t, prov)
			}

			sm := NewActorStateManagerContext(tActorType, tActorID, nil).(*stateManagerCtx)
			sm.stateAsyncProvider = prov

			if test.popTracker != nil {
				test.popTracker(&sm.stateChangeTracker)
			}

			var reply int
			err := sm.Get(context.Background(), test.stateName, &reply)
			assert.Equal(t, test.expErr, err != nil, "unexpected error: %v", err)
			assert.Equal(t, test.expReply, reply)
		})
	}
}
