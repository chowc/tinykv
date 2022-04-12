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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
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
	Match, // 最后一个匹配的 entry index，如果是 leader，就是 log.LastIndex。用于判断 committed。
	Next uint64 // Next: index of next entry to be sent；如果是 leader，就是 Match+1；如果是 Follower，是 AppendEntryResp.Index+1
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
	heartbeatTimeout      int
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := make(map[uint64]*Progress)
	for _, node := range confState.Nodes {
		peers[node] = &Progress{}
	}
	for _, peer := range c.peers {
		peers[peer] = &Progress{}
	}
	l := newLog(c.Storage)
	l.id = c.ID
	r := &Raft{
		id:   c.ID,
		Term: hardState.Term,
		Vote: hardState.Vote,
		RaftLog: l,
		Prs:                   peers,
		// When servers start up, they begin as followers
		State:                 StateFollower,
		votes:                 nil,
		msgs:                  nil,
		Lead:                  0,
		heartbeatTimeout:      c.HeartbeatTick,
		originElectionTimeout: c.ElectionTick,
		electionTimeout:       c.ElectionTick + rand.Intn(c.ElectionTick),
		heartbeatElapsed:      0,
		electionElapsed:       0,
		leadTransferee:        0,
		PendingConfIndex:      0,
	}
	return r
}

func (r *Raft) GetID() uint64 {
	return r.id
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	progress := r.Prs[to]
	prevIndex := progress.Next-1

	prevTerm, _ := r.RaftLog.Term(prevIndex)
	var entries []*pb.Entry
	for idx, ent := range r.RaftLog.entries {
		if ent.Index > prevIndex {
			entries = append(entries, &r.RaftLog.entries[idx])
		}
	}
	log.Debugf("peer [%d] sendAppend -> [%d], prevIndex %d, prevTerm %d, committed %d, applied %d",
		r.id, to, prevIndex, prevTerm, r.RaftLog.committed, r.RaftLog.applied)
	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgAppend,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              prevTerm,
		Index:                prevIndex,
		Entries:              entries,
		Commit:               r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat() {
	// Your Code Here (2A).
	for peer, progress := range r.Prs {
		if peer == r.id {
			continue
		}
		prevIndex := progress.Match
		prevTerm, _ := r.RaftLog.Term(prevIndex)
		r.msgs = append(r.msgs, pb.Message{
			MsgType:  pb.MessageType_MsgHeartbeat,
			From:     r.id,
			To:       peer,
			Term:     r.Term,
			LogTerm:  prevTerm,
			Index:    prevIndex,
			Commit:   r.RaftLog.committed,
		})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// log.Debugf()("tick %+v", *r)
	switch r.State {
	case StateLeader:
		r.tickHeartbeat()
	case StateCandidate:
		r.tickElection()
	case StateFollower:
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
	// log.Debugf()("tickElection [%d]: r.electionElapsed %d, r.electionTimeout %d", r.id, r.electionElapsed, r.electionTimeout)
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
		})
		r.electionElapsed = 0
		r.electionTimeout = r.originElectionTimeout + rand.Intn(r.originElectionTimeout)
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.sendMsg(pb.Message{
			MsgType:  pb.MessageType_MsgHeartbeat,
			From:     r.id,
			Term:     r.Term,
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
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Lead = 0
	r.Vote = r.id
	votes := make(map[uint64]bool)
	votes[r.id] = true
	r.votes = votes
	r.Term++
}

func (r *Raft) sendMsg(m pb.Message) {
	// log.Debugf()("sendMsg [%d]: %+v", r.id, m)
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
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Debugf("[%d] becomeLeader in term [%d], lastindex [%d]", r.id, r.Term, r.RaftLog.LastIndex())
	r.Lead = r.id
	r.State = StateLeader
	r.Vote = None
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex()+1,
	})
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	li := r.RaftLog.LastIndex()
	for peer, progress := range r.Prs {
		if peer == r.id {
			progress.Match = li
			progress.Next = li+1
		} else { // 新 leader 要给 Follower 同步的 index 设为自己最新的日志 index。
			if li == 0 {
				progress.Match = 0
			} else {
				progress.Match = li-1
			}
			progress.Next = li
		}
	}
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
		return nil
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
		return nil
	case pb.MessageType_MsgHup:
		r.campaign()
		return nil
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
		if r.State != StateLeader { // candidate & follower ignore heartbeat
			return nil
		}
		r.sendHeartbeat()
		return nil
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
		return nil
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
		return nil
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
		return nil
	case pb.MessageType_MsgRequestVoteResponse:
		log.Debugf("VoteResponse [%d], msg: %+v", r.id, m)
		if r.State != StateCandidate {
			return nil
		}
		r.votes[m.From] = !m.Reject
		var count, rejectCount int
		for _, v := range r.votes {
			if v {
				count++
			} else {
				rejectCount++
			}
			if count > len(r.Prs)/2 {
				r.becomeLeader()
				return nil
			}
			if rejectCount > len(r.Prs)/2 {
				// TODO: get new leader
				r.becomeFollower(r.Term, 0)
				return nil
			}
		}
		// candidate receive higher term vote
		if m.Term > r.Term || (m.Term == r.Term && m.Commit > r.RaftLog.committed) {
			r.Term = m.Term
			r.becomeFollower(m.Term, m.From)
			return nil
		}
	}
	return nil
}

func (r *Raft) campaign() {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	li := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(li)
	r.sendMsg(pb.Message{
		MsgType:  pb.MessageType_MsgRequestVote,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  term,
		Index:    li,
	})
	log.Debugf("peer [%d] send vote request at term %d, lastindex [%d], lastterm [%d]", r.id, r.Term, li, term)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.Term > m.Term {
		r.rejectVote(m)
		return
	}
	// term 相等的时候，r.Vote = None 说明没投过票；
	// term 不相等的时候，r.Vote = None 说明不是 Candidate（becomeFollower/becomeLeader 会清空 Vote）
	if r.Vote != None && r.Vote != m.From {
		r.rejectVote(m)
		return
	}
	li := r.RaftLog.LastIndex()
	prevTerm, _ := r.RaftLog.Term(li)
	if prevTerm > m.LogTerm || (prevTerm == m.LogTerm && li > m.Index) {
		r.rejectVote(m)
		return
	}
	r.acceptVote(m)
}

func (r *Raft) rejectVote(m pb.Message) {
	li := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(li)
	log.Debugf("[%d] r.Term %d, lastIndex %d, lastTerm %d, rejectVote msg %+v\n", r.id, r.Term, li, term, m)
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgRequestVoteResponse,
		To:       m.From,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   true,
	})
}

func (r *Raft) acceptVote(m pb.Message) {
	li := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(li)
	log.Debugf("peer [%d] acceptVote from [%d] myterm [%d], mylastindex [%d], mylastterm [%d], msg %+v\n",
		r.id, m.From, r.Term, li, term, m)
	r.Vote = m.From
	r.Lead = None
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgRequestVoteResponse,
		To:       m.From,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  term,
		Index:    li,
		Entries:  nil,
		Commit:   r.RaftLog.committed,
	})
}

func (r *Raft) bcastAppend() {
	log.Debugf("[%d] bcastAppend r.Prs: %+v", r.id, r.Prs)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return
	}
	r.electionElapsed = 0
	switch r.State {
	case StateCandidate:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
	case StateLeader:
		// leader 收到 term 更高的消息（AppendRequest）时，
		if m.Term > r.Term || (m.Term == r.Term && m.Commit > r.RaftLog.committed) {
			r.Term = m.Term
			r.becomeFollower(m.Term, m.From)
		}
	case StateFollower:
		// 判断是不是同一个 term，是否已经有 Leader
		if r.Lead == 0 {
			log.Debugf("peer [%d] current term [%d] current leader [%d], new leader [%d], m.term %d", r.id, r.Term, r.Lead, m.From, m.Term)
			r.Lead = m.From // append noop 的时候接受新 leader
		} else if r.Lead != m.From {
			// 可能有另一个 partitio 的 node 与本节点所在的 partition 里的 Leader 有相同的 term，
			// 但是我们不可以接收来自另一个 partition leader 的 append
			log.Debugf("peer [%d] reject current term [%d] current leader [%d], new leader [%d], m.term %d", r.id, r.Term, r.Lead, m.From, m.Term)
			goto reject
		}
		li:= r.RaftLog.LastIndex()
		if m.Index > li { // follower 的日志和 message 的 entries 不能对接上
			goto reject
		}
		if r.Term > m.Term {
			goto reject
		}
		// TODO: 判断之前写入的 log leader 和当前的 m.From
		term, err := r.RaftLog.Term(m.Index)
		if err != nil || (term > 0 && term != m.LogTerm) {
			goto reject
		}
		if m.Index == li { // 刚好能对接上，都是新 entry，不需要判断 index。
			for _, ent := range m.Entries {
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
			}
		} else { // m.Index<li -> follower 日志已经包含了部分 message.entries
			doAppend := false
			for _, ent := range m.Entries {
				if ent.Index <= li { // 判断 m.Entries 对应的 index 在 r.RaftLog 中的 entry 是否匹配，如果不匹配，如果删掉这条以及之后所有的 entry，从而和 leader 保持一致。
					term, err := r.RaftLog.Term(ent.Index)
					if err != nil {
						goto reject
					}
					if doAppend {
						r.RaftLog.entries = append(r.RaftLog.entries, *ent)
						continue
					}
					if term != ent.Term { // delete this entry and all entries after this one
						log.Debugf("peer [%d] TrimToIndex [%d]", r.id, ent.Index)
						r.RaftLog.TrimToIndex(ent.Index)
						r.RaftLog.entries = append(r.RaftLog.entries, *ent)
						// TODO: 这里需要更新 r.RaftLog.storage？
						r.RaftLog.stabled = ent.Index-1
						doAppend = true
					}
					continue
				}
				r.RaftLog.entries = append(r.RaftLog.entries, *ent)
			}
		}

		// TODO: applied <= committed <= stabled，这里会出现 committed>stabled 的情况，如何处理？
		// follower 的 committed 不能大于 leader 同步过来的最大 index。
		// 这里可能 m.Index < r.RaftLog.stabled：因为 follower 自身的日志还没有和 leader 的完全同步；
		// 如果 m.Entries 是空的，那么 committed 要 <= m.Index，因为 follower 还没有接收到 leader 同步过来的大于 m.Index 的 entries（如果已经接收过了，这里 m.Index 就应该是同步过的最大 entry index）
		// 如果 m.Entries 不是空的，那么 committed <= m.Index+len(m.Entries)，因为通过这次同步，follower 可以保证所有 <= m.Index+len(m.Entries) 都同步成功了。
		newCommitted := min(m.Commit, m.Index+uint64(len(m.Entries)))
		if newCommitted > r.RaftLog.committed {
			r.RaftLog.committed = newCommitted
		}
		li = r.RaftLog.LastIndex()

		term, _ = r.RaftLog.Term(li)
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgAppendResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              term,
			Index:                li, // 当前确认收到的最大 index
		})
		return
	}
reject:
	li := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(li)
	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgAppendResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              term,
		Index:                li,
		Reject:               true,
	})
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	progress := r.Prs[m.From]
	if m.Reject == true {
		// TODO: binary search
		if progress.Match > 0 {
			progress.Match -= 1
		}
		if progress.Next > 1 {
			progress.Next -= 1
		}
		r.sendAppend(m.From)
		return
	}
	// only log entries from the leader’s current term are committed by counting replicas.
	term, _ := r.RaftLog.Term(m.Index)
	if term != r.Term {
		r.bcastAppend()
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	if progress.Match < m.Index {
		progress.Match = m.Index
		progress.Next = m.Index+1
	}

	committedCount := make(map[uint64]int)

	for _, progress := range r.Prs {
		committedCount[progress.Match]++
	}
	var keys []int
	for k := range committedCount {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	count := 0
	for i:=len(keys)-1; i>=0; i-- {
		committedCount[uint64(keys[i])] += count
		count += committedCount[uint64(keys[i])]
	}
	var committed uint64
	for i:=len(keys)-1; i>=0; i-- {
		if committedCount[uint64(keys[i])] > len(r.Prs)/2 {
			committed = uint64(keys[i])
			break
		}
	}
	if r.RaftLog.committed < committed {
		r.RaftLog.committed = committed
		r.bcastAppend()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		log.Debugf("peer [%d] r.Term [%d] > [%d] m.Term [%d], reject heartbeat", r.id, r.Term, m.Term, m.From)
		return
	}
	r.electionElapsed = 0
	li := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(li)
	if r.State == StateCandidate && r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if li < m.Commit { // 缺少日志
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgAppendResponse, // 触发 leader 重发 AppendRequest
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              term,
			Index:                li,
			Reject:               true,
		})
		return
	}
	committed := min(m.Index, m.Commit)
	if r.RaftLog.committed < committed {
		r.RaftLog.committed = committed
	}
	r.Term = m.Term
	r.Lead = m.From
	// 'MessageType_MsgHeartbeat' sends heartbeat from leader. When 'MessageType_MsgHeartbeat' is passed
	//	to candidate and message's term is higher than candidate's, the candidate
	//	reverts back to follower and updates its committed index from the one in
	//	this heartbeat. And it sends the message to its mailbox. When
	//	'MessageType_MsgHeartbeat' is passed to follower's Step method and message's term is
	//	higher than follower's, the follower updates its leaderID with the ID
	//	from the message.
	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgHeartbeatResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              term,
		Index:                li,
	})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State != StateLeader {
		return
	}
	for idx := range m.Entries {
		m.Entries[idx].Term = r.Term
		m.Entries[idx].Index = r.RaftLog.LastIndex()+1
		log.Debugf("peer [%d] propose msg index: [%d], data %v", r.id, m.Entries[idx].Index, m.Entries[idx].Data)
		m.LogTerm = r.Term

		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: m.Entries[idx].EntryType,
			Term:      m.Entries[idx].Term,
			Index:     m.Entries[idx].Index,
			Data:      m.Entries[idx].Data,
		})
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex()+1
	if len(r.Prs) == 1 { // 只有一个节点，直接 commit
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}
	r.bcastAppend()
}

func (r *Raft) Advance(newStabled, newApplied, msgsLen uint64) {
	if r.RaftLog.applied < newApplied {
		r.RaftLog.applied = newApplied
	}
	if r.RaftLog.stabled < newStabled {
		r.RaftLog.stabled = newStabled
	}
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
