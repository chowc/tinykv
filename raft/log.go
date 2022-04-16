// Copyright 2015 The etcd Authors
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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// 已经 committed 的要保证已经持久化，而持久化的非 committed 可以被删除（例如：peer_storage.Append 会覆盖）
	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact. 一部分可能已经是 stable 的。
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	id uint64

	compactIndex uint64
	compactTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	fi, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	li, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, _ := storage.Entries(fi, li+1)
	hardState, _, _ := storage.InitialState()
	log.Debugf("peer newLog committed %d, applied %d", hardState.Commit, fi-1)
	commit := max(hardState.Commit, fi-1)
	return &RaftLog{
		storage:         storage,
		committed:       commit,
		applied:         fi-1,
		stabled:         li,
		entries:         entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// 从 storage 读取当前的 index 区间
	log.Debugf("peer [%d] maybeCompact", l.id)
	if len(l.entries) == 0 {
		return
	}
	fi, _ := l.storage.FirstIndex()
	log.Debugf("peer [%d] maybeCompact, fi %d, len(entries) %d, first entry %v", l.id, fi, len(l.entries), l.entries[0])
	if fi > l.entries[0].Index {
		idx := l.sliceIdx(fi)
		if idx == -1 {
			l.entries = nil
			return
		}

		l.entries = l.entries[idx+1:]
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// TODO: 这里包不包括 l.stabled？
	// stabled 是 entry index
	var begin int
	for idx, ent := range l.entries {
		if ent.Index == l.stabled {
			begin = idx+1
		}
	}
	if begin >= len(l.entries) {
		return []pb.Entry{}
	}
	return l.entries[begin:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	// log[applied: committed]
	var entries []pb.Entry
	for idx, ent := range l.entries {
		if ent.Index > l.applied && ent.Index <= l.committed {
			entries = append(entries, l.entries[idx])
		}
	}
	log.Debugf("peer [%d] nextEnts l.committed %d l.applied %d, len(entries) %d", l.id, l.committed, l.applied, len(entries))

	return entries
}

// LastIndex return the last index of the log entries. next entry index should +1
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return max(l.entries[len(l.entries)-1].Index, l.compactIndex)
	}
	li, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}

	return max(li, l.compactIndex)
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	if i < l.compactIndex {
		return 0, ErrCompacted
	}
	if i == l.compactIndex {
		return l.compactTerm, nil
	}
	if len(l.entries) > 0 && l.entries[0].Index <= i {
		idx := i-l.entries[0].Index
		if idx >= uint64(len(l.entries)) {
			return 0, nil
		}
		return l.entries[idx].Term, nil
	}
	return l.storage.Term(i)
}

// TrimToIndex deletes entries whose index >= end
func (l *RaftLog) TrimToIndex(end uint64) {
	for idx := range l.entries {
		if l.entries[idx].Index == end {
			l.entries = l.entries[0:idx]
			return
		}
	}
}

func (l *RaftLog) Info() (uint64, uint64) {
	return l.applied, l.committed
}

func (l *RaftLog) sliceIdx(index uint64) int {
	for i, ent := range l.entries {
		if ent.Index == index {
			return i
		}
	}
	return -1
}