package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	result, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}
	if result != nil {
		// region may change when snapshot is applied
		if !reflect.DeepEqual(result.PrevRegion, result.Region) {
			d.peerStorage.SetRegion(result.Region)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[result.Region.Id] = result.Region
			storeMeta.regionRanges.Delete(&regionItem{region: result.PrevRegion})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: result.Region})
			storeMeta.Unlock()
		}
	}
	d.Send(d.ctx.trans, rd.Messages)
	if len(rd.CommittedEntries) == 0 {
		d.RaftGroup.Advance(rd)
		return
	}
	raftID := d.RaftGroup.Raft.GetID()

	log.Debugf("peer [%d] HandleRaftReady: entries %d committed entry %d", raftID, len(rd.Entries), len(rd.CommittedEntries))

	wb := &engine_util.WriteBatch{}
	// 这里存在一个特殊情况，就是所谓的“空日志”。在 raft-rs 的实现中，当选举出新的 Leader 时，新 Leader 会广播一条“空日志”，
	// 以提交前面 term 中的日志（详情请见 Raft 论文）。此时，可能还有一些在前面 term 中提出的 proposal 仍然处于 pending 阶段，
	// 而因为有新 Leader 产生，这些 proposal 永远不可能被确认了，
	// 因此我们需要对它们进行清理，以免关联的 callback无法调用导致一些资源无法释放。
	for _, ent := range rd.CommittedEntries {
		d.handleCommittedEntry(ent, wb)
		if d.stopped {
			return
		}
		if err := d.peerStorage.Engines.WriteKV(wb); err != nil {
			log.Errorf("WriteKV fail: %+v", err)
		}
		wb.Reset()
	}
	d.peerStorage.applyState.AppliedIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	log.Debugf("peer [%d] set AppliedIndex to %d, d.regiondId %d", raftID, d.peerStorage.applyState.AppliedIndex, d.regionId)
	if err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		log.Errorf("SetMeta fail: %v", err)
	}
	if err := d.peerStorage.Engines.WriteKV(wb); err != nil {
		log.Errorf("WriteKV fail: %+v", err)
	}
	d.RaftGroup.Advance(rd)
}

func getRequestKey(req *raft_cmdpb.Request) []byte {
	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		return req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		return req.Delete.Key
	case raft_cmdpb.CmdType_Get:
		return req.Get.Key

	}
	return nil
}

func (d *peerMsgHandler) handleCommittedEntry(ent eraftpb.Entry, wb *engine_util.WriteBatch) {
	var msg raft_cmdpb.RaftCmdRequest
	raftID := d.Meta.Id
	if d.isConfChangePropose(ent) {
		log.Debugf("handle admin propose")
		switch ent.EntryType {
		case eraftpb.EntryType_EntryConfChange:
			var cc eraftpb.ConfChange
			if err := cc.Unmarshal(ent.Data); err != nil {
				log.Errorf("Unmarshal fail: %v", err)
				panic(err)
			}
			log.Debugf("peer [%d] change peer %v", d.PeerId(), cc.NodeId)
			if err := msg.Unmarshal(cc.Context); err != nil {
				panic(err)
			}
			log.Debugf("peer [%d] handle admin request %v", raftID, msg)
			d.handleAdminRequest(&msg, &cc, wb)
			d.handlePropose(ent, func(proposal *proposal) {
				resp := &raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{},
				}
				resp.AdminResponse = &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
					ChangePeer: &raft_cmdpb.ChangePeerResponse{
						Region: d.Region(),
					},
				}
				proposal.cb.Done(resp)
			})
		}
		return
	}
	if err := msg.Unmarshal(ent.Data); err != nil {
		log.Errorf("Unmarshal fail: %v", err)
		panic(err)
	}
	log.Debugf("peer [%d] HandleRaftReady: apply entry to raft db: index [%d], msg %v", raftID, ent.Index, msg)

	for _, request := range msg.Requests { // 非 leader 没有 proposal，所以要先 apply 到 kvdb
		switch request.CmdType {
		case raft_cmdpb.CmdType_Put:
			log.Debugf("peer [%d] apply PUT entry index [%d] request key %v, value: %v",
				raftID, ent.Index, request.Put.Key, request.Put.Value)
			wb.SetCF(request.Put.Cf, request.Put.Key, request.Put.Value)
		case raft_cmdpb.CmdType_Delete:
			wb.DeleteCF(request.Delete.Cf, request.Delete.Key)
		}
	}
	if msg.AdminRequest != nil {
		d.handleAdminRequest(&msg, nil, wb)
	}
	d.handlePropose(ent, func(proposal *proposal) {
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}
		log.Debugf("peer [%d] handlePropose: proposal %v, msg: %v", raftID, proposal, msg)

		for _, request := range msg.Requests {
			switch request.CmdType {
			case raft_cmdpb.CmdType_Get:
				d.peerStorage.applyState.AppliedIndex = ent.Index
				log.Debugf("peer [%d] set AppliedIndex to %d", raftID, d.peerStorage.applyState.AppliedIndex)
				if err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
					log.Errorf("SetMeta fail: %v", err)
					proposal.cb.Done(ErrResp(err))
				}
				if err := d.peerStorage.Engines.WriteKV(wb); err != nil {
					log.Errorf("WriteKV fail: %+v", err)
					proposal.cb.Done(ErrResp(err))
				}
				wb = &engine_util.WriteBatch{}
				val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, request.Get.Cf, request.Get.Key)
				if err == badger.ErrEmptyKey {
					val = nil
				} else if err != nil {
					log.Errorf("GetCF fail: %v", err)
					proposal.cb.Done(ErrResp(err))
				}
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get: &raft_cmdpb.GetResponse{
						Value: val,
					},
				})

			case raft_cmdpb.CmdType_Put:
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})

			case raft_cmdpb.CmdType_Delete:
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
			case raft_cmdpb.CmdType_Snap:
				if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
					proposal.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
					return
				}
				d.peerStorage.applyState.AppliedIndex = ent.Index
				log.Debugf("peer [%d] set AppliedIndex to %d", raftID, d.peerStorage.applyState.AppliedIndex)
				if err := wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
					log.Errorf("SetMeta fail: %v", err)
					proposal.cb.Done(ErrResp(err))
				}
				if err := d.peerStorage.Engines.WriteKV(wb); err != nil {
					log.Errorf("WriteKV fail: %+v", err)
					proposal.cb.Done(ErrResp(err))
				}
				wb = &engine_util.WriteBatch{}
				proposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				applied, committed := d.RaftGroup.Raft.RaftLog.Info()
				log.Debugf("peer [%d] handle snap [applied, committed]: [%d, %d]", raftID, applied, committed)
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap: &raft_cmdpb.SnapResponse{
						Region: d.Region(),
					},
				})
			}
		}
		log.Debugf("peer [%d] done apply entry index [%d]", raftID, ent.Index)
		proposal.cb.Done(resp)
	})
}

func (d *peerMsgHandler) handlePropose(ent eraftpb.Entry, fn func(proposal *proposal)) {
	if len(d.proposals) > 0 {
		proposal := d.proposals[0]
		if proposal.index == ent.Index && proposal.term == ent.Term {
			fn(proposal)
		} else if proposal.index == ent.Index && proposal.term != ent.Term {
			NotifyStaleReq(proposal.term, proposal.cb)
		}
		d.proposals = d.proposals[1:]
	}
}

func (d *peerMsgHandler) isConfChangePropose(ent eraftpb.Entry) bool {
	return ent.EntryType == eraftpb.EntryType_EntryConfChange
}

// HandleMsg processes all the messages received from raftCh, including MsgTypeTick which calls RawNode.Tick() to drive the Raft,
// MsgTypeRaftCmd which wraps the request from clients and MsgTypeRaftMessage which is the message transported between Raft peers.
// All the message types are defined in kv/raftstore/message/msg.go.
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {

		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	if msg.Header.RegionId != d.regionId {
		cb.Done(ErrRespRegionNotFound(msg.Header.RegionId))
		return
	}
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			resp := &raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType:        msg.AdminRequest.CmdType,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				},
			}
			cb.Done(resp)
			return
		}
	}
	d.propose(msg, cb)
}

func (d *peerMsgHandler) propose(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_ChangePeer:
			req := msg.AdminRequest.ChangePeer
			li := d.nextProposalIndex()
			term := d.Term()
			p := &proposal{
				index: li,
				term:  term,
				cb:    cb,
			}
			log.Debugf("peer [%d] propose changepeer %v, entry index %d", d.PeerId(), msg, li)
			d.proposals = append(d.proposals, p)
			bs, err := msg.Marshal()
			if err != nil {
				panic(err)
			}
			cc := eraftpb.ConfChange{
				ChangeType: req.ChangeType,
				NodeId:     req.Peer.Id,
				Context:    bs,
			}
			if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
				cb.Done(ErrResp(err))
			}
			return
		}
	}
	data, err := msg.Marshal()
	if err != nil {
		log.Errorf("msg Marshal fail: %v", err)
		cb.Done(ErrResp(err))
		return
	}
	log.Debugf("peer [%d] proposeRaftCommand: %v", d.RaftGroup.Raft.GetID(), msg)
	li := d.nextProposalIndex()
	term := d.Term()
	p := &proposal{
		index: li,
		term:  term,
		cb:    cb,
	}
	d.proposals = append(d.proposals, p)
	log.Debugf("peer [%d] proposeRaftCommand: add proposal %v, msg %v", d.RaftGroup.Raft.GetID(), p, msg)
	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) handleAdminRequest(msg *raft_cmdpb.RaftCmdRequest, change *eraftpb.ConfChange, wb *engine_util.WriteBatch) {
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		log.Debugf("peer [%d] handle compact log %v", d.PeerId(), msg)
		compact := msg.AdminRequest.CompactLog
		if compact.CompactIndex >= d.peerStorage.applyState.TruncatedState.Index {
			d.peerStorage.applyState.TruncatedState = &rspb.RaftTruncatedState{
				Index: compact.CompactIndex,
				Term:  compact.CompactTerm,
			}
			wb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			d.ScheduleCompactLog(compact.CompactIndex)
		}
	case raft_cmdpb.AdminCmdType_ChangePeer:
		log.Debugf("peer [%d] change peer %v", d.PeerId(), change)

		if change.ChangeType == eraftpb.ConfChangeType_RemoveNode && d.PeerId() == change.NodeId {
			log.Debugf("peer [%d] destroyPeer %v", d.PeerId(), change.NodeId)
			d.destroyPeer()
			return
		}
		switch change.ChangeType {
		case eraftpb.ConfChangeType_AddNode:
			newPeer := msg.AdminRequest.ChangePeer.Peer

			peer := util.FindPeer(d.Region(), newPeer.StoreId)
			if peer != nil {
				return
			}

			d.Region().RegionEpoch.ConfVer++

			d.Region().Peers = append(d.Region().Peers, newPeer)
			log.Debugf("peer [%d] confver incre %v, peers %v", d.peer.PeerId(), d.Region(), d.Region().Peers)
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.setRegion(d.Region(), d.peer)
			storeMeta.Unlock()
			d.insertPeerCache(newPeer)
		case eraftpb.ConfChangeType_RemoveNode:
			deletePeer := msg.AdminRequest.ChangePeer.Peer
			oldPeer := util.RemovePeer(d.Region(), deletePeer.StoreId)
			if oldPeer == nil {
				return
			}
			d.Region().RegionEpoch.ConfVer++

			log.Debugf("peer [%d] confver incre %v, peers %v", d.peer.PeerId(), d.Region(), d.Region().Peers)
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.setRegion(d.Region(), d.peer)
			storeMeta.Unlock()
			d.removePeerCache(change.NodeId)
		}
		d.RaftGroup.ApplyConfChange(*change)
	case raft_cmdpb.AdminCmdType_Split:
		split := msg.AdminRequest.Split
		log.Debugf("peer [%d] handle split admin request, split %v", d.PeerId(), split)
		// CmdType: raft_cmdpb.AdminCmdType_Split,
		// 	Split: &raft_cmdpb.SplitRequest{
		// 	SplitKey:    t.SplitKey,
		// 	NewRegionId: resp.NewRegionId,
		// 	NewPeerIds:  resp.NewPeerIds,
		// },
		d.Region().RegionEpoch.Version++
		newRegion := metapb.Region{
			Id:       split.NewRegionId,
			StartKey: split.SplitKey,
			EndKey:   d.Region().EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: func() []*metapb.Peer {
				var peers []*metapb.Peer
				for _, o := range split.NewPeerIds {
					peers = append(peers, &metapb.Peer{
						Id:      o,
						StoreId: d.storeID(),
					})
				}
				log.Debugf("peer [%d] new peers %+v", d.PeerId(), peers)
				return peers
			}(),
		}

		p, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.schedulerTaskSender, d.ctx.engine, &newRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(p)
		d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
		state := new(rspb.RegionLocalState)
		state.Region = &newRegion
		kvWB := new(engine_util.WriteBatch)
		kvWB.SetMeta(meta.PrepareBootstrapKey, state)
		kvWB.SetMeta(meta.RegionStateKey(newRegion.Id), state)
		writeInitialApplyState(kvWB, newRegion.Id)
		raftWB := new(engine_util.WriteBatch)
		writeInitialRaftState(raftWB, newRegion.Id)
		d.Region().EndKey = split.SplitKey
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[split.NewRegionId] = &newRegion
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: &newRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		storeMeta.Unlock()
		log.Debugf("storeMeta.regionRanges %+v", storeMeta.regionRanges)
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	// compact range [d.LastCompactedIdx, truncatedIndex + 1]
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		log.Debugf("peer [%d] d.Region %v", d.Meta.Id, d.Region())
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
