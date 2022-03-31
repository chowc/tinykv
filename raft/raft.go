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
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	originElectionTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	TickFn func(r *Raft)
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	peers := make(map[uint64]*Progress)
	for _, peer := range c.peers {
		peers[peer] = &Progress{}
	}
	return &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          &RaftLog{
			storage:         c.Storage,
			committed:       0,
			applied:         c.Applied,
			stabled:         0,
			entries:         nil,
			pendingSnapshot: nil,
		},
		Prs:              peers,
		State:            StateFollower,
		votes:            nil,
		msgs:             nil,
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		originElectionTimeout: c.ElectionTick,
		electionTimeout:  c.ElectionTick+rand.Intn(c.ElectionTick),
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
		TickFn: tickElection,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgAppend,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                0,
		Entries:              nil,
		Commit:               r.RaftLog.committed,
		Snapshot:             nil,
		Reject:               false,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	r.sendMsg(pb.Message{
			MsgType:              pb.MessageType_MsgHeartbeat,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              logTerm,
			Index:                r.RaftLog.LastIndex(),
			Entries:              nil,
			Commit:               r.RaftLog.committed,
			Snapshot:             nil,
			Reject:               false,
		})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	r.TickFn(r)
}

func tickElection(r *Raft)  {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.becomeCandidate()
		r.sendMsg(pb.Message{
				MsgType:              pb.MessageType_MsgRequestVote,
				From:                 r.id,
				Term:                 r.Term,
				LogTerm:              0,
				Index:                0,
				Entries:              nil,
				Commit:               r.RaftLog.committed,
				Snapshot:             nil,
				Reject:               false,
			})
		r.electionElapsed = 0
	}
}

func tickHeartbeat(r *Raft)  {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.sendMsg(pb.Message{
				MsgType:              pb.MessageType_MsgHeartbeat,
				From:                 r.id,
				Term:                 r.Term,
				LogTerm:              0,
				Index:                r.RaftLog.committed,
				Entries:              nil,
				Commit:               0,
				Snapshot:             nil,
				Reject:               false,
			})
		r.heartbeatElapsed = 0
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.TickFn = tickElection
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Lead = 0
	r.Vote = r.id
	r.Vote = r.id
	votes := make(map[uint64]bool)
	votes[r.id] = true
	r.votes = votes
	r.Term++
	r.TickFn = tickElection
	r.electionTimeout = r.originElectionTimeout+rand.Intn(r.originElectionTimeout)
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) sendMsg(m pb.Message) {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		m.To = peer
		r.msgs = append(r.msgs, m)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.Lead = r.id
	r.State = StateLeader
	r.TickFn = tickHeartbeat
	r.sendMsg(pb.Message{
		MsgType:              pb.MessageType_MsgPropose,
		To:                   0,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                r.RaftLog.committed,
		Entries:              nil,
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
	})
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term || m.From == r.id || (m.Term == r.Term && r.Lead == 0) {
			// 没有投过票或者是自己的 Vote 请求或者是重复的 Vote 消息（要幂等）
			if m.Term > r.Term {
				r.Term = m.Term
				r.Vote = 0
			}
			if r.Vote == 0 || r.Vote == m.From {
				r.Vote = m.From
				r.msgs = append(r.msgs, pb.Message{
					MsgType:              pb.MessageType_MsgRequestVoteResponse,
					To:                   m.From,
					From:                 r.id,
					Term:                 m.Term,
					LogTerm:              0,
					Index:                0,
					Entries:              nil,
					Commit:               0,
					Snapshot:             nil,
					Reject:               false,
				})
				if r.id != m.From {
					r.becomeFollower(m.Term, m.From)
				}
				return nil
			}
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgRequestVoteResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 m.Term,
			LogTerm:              0,
			Index:                0,
			Entries:              nil,
			Commit:               0,
			Snapshot:             nil,
			Reject:               true,
		})
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			r.becomeCandidate()
			r.sendMsg(pb.Message{
				MsgType:              pb.MessageType_MsgRequestVote,
				From:                 r.id,
				Term:                 r.Term,
				LogTerm:              0,
				Index:                0,
				Entries:              nil,
				Commit:               r.RaftLog.committed,
				Snapshot:             nil,
				Reject:               false,
			})
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.sendMsg(pb.Message{
				MsgType:              pb.MessageType_MsgHeartbeat,
				From:                 r.id,
				Term:                 r.Term,
				LogTerm:              0,
				Index:                0,
				Entries:              nil,
			})
		}
	}
	switch r.State {
	case StateFollower:
		if m.Term > r.Term || (m.Term == r.Term && m.Commit > r.RaftLog.committed) {
			r.Term = m.Term
			return nil
		}
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			if m.Term == r.Term && !m.Reject {
				r.votes[m.From] = true
				count := 0
				for _, v := range r.votes {
					if v {
						count++
					}
					if count > len(r.Prs)/2 {
						r.becomeLeader()
					}
				}
			}
		}
		if m.Term > r.Term || (m.Term == r.Term && m.Commit > r.RaftLog.committed) {
			r.Term = m.Term
			r.becomeFollower(m.Term, m.From)
			return nil
		}
	case StateLeader:
		if m.Term > r.Term || (m.Term == r.Term && m.Commit > r.RaftLog.committed) {
			r.Term = m.Term
			r.becomeFollower(m.Term, m.From)
			return nil
		}
		if m.MsgType == pb.MessageType_MsgRequestVote {
			if m.Term > r.Term || (m.Term == r.Term && m.Commit > r.RaftLog.committed) {
				r.Term = m.Term
				r.becomeFollower(m.Term, m.From)
				return nil
			}
		}
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	switch r.State {
	case StateCandidate:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgHeartbeatResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,
		Index:                r.RaftLog.committed,
		Entries:              nil,
		Commit:               0,
		Snapshot:             nil,
		Reject:               false,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
