// Copyright 2026 The etcd Authors
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

package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/IkhsanovIS/qraft/v3/raftpb"
)

// roleChanStatus synchronizes with the run loop after a message has been
// processed and the corresponding role notification has been published.
func roleChanStatus(t *testing.T, n *node) Status {
	t.Helper()
	result := make(chan Status, 1)
	go func() { result <- n.Status() }()
	select {
	case status := <-result:
		return status
	case <-time.After(time.Second):
		t.Fatal("node stalled while publishing a role")
		return Status{}
	}
}

func roleChanStop(t *testing.T, n *node) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		n.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("node did not stop with unread role notifications")
	}
}

func roleChanElect(t *testing.T, n *node, storage *MemoryStorage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, n.Campaign(ctx))
	for {
		rd := readyWithTimeout(n)
		require.NoError(t, storage.Append(rd.Entries))
		n.Advance()
		if rd.SoftState != nil && rd.RaftState == StateLeader {
			break
		}
	}
	require.Equal(t, StateLeader, roleChanStatus(t, n).RaftState)
}

func TestNodeRoleChanLatestUnread(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1))
	n := newNode(newTestRawNode(1, 10, 1, storage))
	go n.run()
	t.Cleanup(func() { roleChanStop(t, &n) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Never consume RoleChan while the node repeatedly wins and loses
	// leadership. Publishing must not stall Raft or accumulate old roles.
	for i := 0; i < 4; i++ {
		roleChanElect(t, &n, storage)
		status := roleChanStatus(t, &n)
		require.NoError(t, n.Step(ctx, raftpb.Message{
			Type: raftpb.MsgHeartbeat, From: 2, Term: status.Term + 1,
		}))
		require.Equal(t, StateFollower, roleChanStatus(t, &n).RaftState)
	}
	require.Equal(t, 1, n.RoleChan().Len())

	// Stop must complete without waiting for the pending role to be read,
	// even when multiple callers stop the node concurrently.
	var callers sync.WaitGroup
	for i := 0; i < 4; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			n.Stop()
		}()
	}
	done := make(chan struct{})
	go func() { callers.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("concurrent Stop calls stalled")
	}

	// Closing the ring preserves its latest unread value, then closes Out.
	select {
	case role, ok := <-n.RoleChan().Out():
		require.True(t, ok)
		require.Equal(t, NOT_LEADER, role)
	case <-ctx.Done():
		t.Fatal("latest unread role was lost on Stop")
	}
	select {
	case _, ok := <-n.RoleChan().Out():
		require.False(t, ok, "only the latest unread role should remain")
	case <-ctx.Done():
		t.Fatal("role channel did not close after draining")
	}
}

func TestNodeRoleChanNonLeaderTransitions(t *testing.T) {
	n := newNode(newTestRawNode(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3))))
	go n.run()
	t.Cleanup(func() { roleChanStop(t, &n) })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	select {
	case role := <-n.RoleChan().Out():
		require.Equal(t, NOT_LEADER, role)
	case <-ctx.Done():
		t.Fatal("initial role was not published")
	}

	// Becoming a candidate still means NOT_LEADER; it must not produce
	// another role notification.
	require.NoError(t, n.Campaign(ctx))
	require.Equal(t, StateCandidate, roleChanStatus(t, &n).RaftState)
	require.Zero(t, n.RoleChan().Len())

	// Returning to follower and changing the remote leader likewise do not
	// change this node's leader role.
	for _, leader := range []uint64{2, 3} {
		require.NoError(t, n.Step(ctx, raftpb.Message{
			Type: raftpb.MsgHeartbeat, From: leader, Term: leader,
		}))
		status := roleChanStatus(t, &n)
		require.Equal(t, StateFollower, status.RaftState)
		require.Equal(t, leader, status.Lead)
		require.Zero(t, n.RoleChan().Len())
	}
}

func TestNodeRoleChanInitialLeader(t *testing.T) {
	storage := newTestMemoryStorage(withPeers(1))
	rn := newTestRawNode(1, 10, 1, storage)
	require.NoError(t, rn.Campaign())
	for rn.raft.state != StateLeader {
		rd := rn.Ready()
		require.NoError(t, storage.Append(rd.Entries))
		rn.Advance(rd)
	}

	// Construct Node around an already elected RawNode. Its initial role
	// must reflect the actual state instead of assuming NOT_LEADER.
	n := newNode(rn)
	go n.run()
	t.Cleanup(func() { roleChanStop(t, &n) })
	select {
	case role := <-n.RoleChan().Out():
		require.Equal(t, LEADER, role)
	case <-time.After(time.Second):
		t.Fatal("initial leader role was not published")
	}
	require.Equal(t, StateLeader, roleChanStatus(t, &n).RaftState)
	require.Zero(t, n.RoleChan().Len())
}
