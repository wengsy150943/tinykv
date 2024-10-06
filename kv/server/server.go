package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

func (server *Server) startMVCCTxn(cont *kvrpcpb.Context, ts uint64) (*mvcc.MvccTxn, error) {
	reader, err := server.storage.Reader(cont)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, ts)
	return txn, nil
}

// Transactional API.
// start a txn to read the value of the key
// set not found if the value is not found
// return err if the key is locked
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	ret := &kvrpcpb.GetResponse{}
	key := req.GetKey()
	ts := req.GetVersion()

	// start a txn
	txn, err := server.startMVCCTxn(req.Context, ts)
	if err != nil {
		return ret, err
	}	

	// check lock
	lock, err := txn.GetLock(key)
	// if block, return err
	if err != nil || (lock != nil && lock.Ts < ts) {
		if lock != nil && lock.Ts < ts {
			ret.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
		}
		return ret, err
	}

	// get value
	val, err := txn.GetValue(key)
	// set not found
	if val == nil {
		ret.NotFound = true
	} else{
		ret.Value = val
	}
	return ret, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	ret := &kvrpcpb.PrewriteResponse{}
	ts := req.GetStartVersion()

	// start a txn
	txn, err := server.startMVCCTxn(req.Context, ts)
	if err != nil {
		return ret, err
	}	

	// mutation contains all mutations of this txn
	// check if can prewrite
	for _, mutation := range req.Mutations {
		key := mutation.Key

		// check lock
		// lock not nil, meaning that other txn has locked this key
		_lock, _err := txn.GetLock(key)
		if _err != nil || _lock != nil{
			if _lock != nil  {
				ret.Errors = append(ret.Errors,  &kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    ts,
						ConflictTs: _lock.Ts,
						Key:        key,
						Primary:    req.PrimaryLock,
					},
				})
			}
			return ret, _err
		}	
		
		// check write
		_write, _ts, _err := txn.MostRecentWrite(key)
		// write ts >= ts, meaning that other txn has written this key
		if _err != nil || _ts > ts {
			if _ts > ts {// not err, but a newer txn write
				ret.Errors = append(ret.Errors,  &kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    ts,
						ConflictTs: _write.StartTS,
						Key:        key,
						Primary:    req.PrimaryLock,
					},
				})
			}
			return ret, _err
		}
	}

	// lock all keys
	for _, mutation := range req.Mutations {
		key := mutation.Key
		op := mutation.Op
		value := mutation.Value
		// put different lock
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts: ts,
			Ttl: req.LockTtl,
			Kind: mvcc.WriteKindFromProto(op),
		}
		txn.PutLock(key, lock)

		// txn write
		switch op {
		case kvrpcpb.Op_Put:
			txn.PutValue(key, value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(key)
		}		
	}

	// write all keys
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	ret := &kvrpcpb.CommitResponse{}
	ts := req.GetStartVersion()

	// start a txn
	txn, err := server.startMVCCTxn(req.Context, ts)
	if err != nil {
		return ret, err
	}	

	// add latch
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		// check lock
		_lock, _err := txn.GetLock(key)
		if _err != nil{
			return ret, err
		}
		// lock is nil, meaning that it is release by another req
		if _lock == nil {
			_write, _, _err := txn.CurrentWrite(key)
			if _err != nil{
				return ret, _err
			}
			// but write is still rollback, conflict
			if _write != nil && _write.Kind == mvcc.WriteKindRollback {
				ret.Error = &kvrpcpb.KeyError{}
			}
			return ret, nil
		}

		// lock not nil and ts is different, meaning that other txn has locked this key
		if _lock.Ts != ts {
			ret.Error = &kvrpcpb.KeyError{
				Retryable: "true",
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    ts,
					ConflictTs: _lock.Ts,
					Key:        key,
					Primary:    _lock.Primary,
				},
			}
			return ret, _err
		}
		
		// append a write
		newWrite := &mvcc.Write{
			StartTS: ts,
			Kind:    _lock.Kind,
		}
		txn.PutWrite(key, req.CommitVersion, newWrite)
		txn.DeleteLock(key)
	}

	// write all keys and locks
	err = server.storage.Write(req.Context, txn.Writes())
	return ret, err
}

// KvScan is the transactional equivalent of RawScan, it reads many values from the database.
// But like KvGet, it does so at a single point in time. Because of MVCC, KvScan is significantly more complex than RawScan -
// you can't rely on the underlying storage to iterate over values because of multiple versions and key encoding.
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	ret := &kvrpcpb.ScanResponse{}
	ts := req.GetVersion()

	// start a txn
	txn, err := server.startMVCCTxn(req.Context, ts)
	if err != nil {
		return ret, err
	}	

	scanner := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scanner.Close()

	// scan all keys
	for i := uint32(0); i < req.GetLimit(); i++ {
		_key, _value, err := scanner.Next()
		// Note, if all ret is nil, meaning that scanner is exhausted
		if _key == nil && _value == nil && err == nil {
			break
		}
		// if err, skip
		if err != nil {
			return ret, err
		}

		pair := kvrpcpb.KvPair{}
		// similar with KvGet
		_lock, err := txn.GetLock(_key)
		if err != nil {
			return ret, err
		}
		// if block, append err
		if _lock != nil && _lock.Ts < ts{
			pair.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: _lock.Primary,
					LockVersion: _lock.Ts,
					Key:         _key,
					LockTtl:     _lock.Ttl,
				},
			}
			ret.Pairs = append(ret.Pairs, &pair)
			return ret, nil
		} 
		if _value != nil {// otherwise, append value if exists
			pair.Key = _key
			pair.Value = _value
			ret.Pairs = append(ret.Pairs, &pair)
		}
		
	}

	return ret, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	ret := &kvrpcpb.CheckTxnStatusResponse{}
	ts := req.GetCurrentTs()
	lockTs := req.GetLockTs()
	key := req.GetPrimaryKey()

	// start a txn
	txn, err := server.startMVCCTxn(req.Context, lockTs)
	if err != nil {
		return ret, err
	}	

	// check write
	write, commitTs, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}
	// if write is not nil, meaning that txn's info is still in memory, so nothing to do, just collect info
	if write != nil && commitTs > 0{
		// it is rollback
		if write.Kind == mvcc.WriteKindRollback {
			ret.CommitVersion = 0
		} else {
			ret.CommitVersion = commitTs
		}
		ret.Action = kvrpcpb.Action_NoAction
		return ret, nil
	}

	// check the lock
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	// if lock is nil or timeout, rollback
	timeout := lock != nil && (mvcc.PhysicalTime(lockTs) + lock.Ttl < mvcc.PhysicalTime(ts))
	if lock == nil || timeout {
		if lock == nil{
			ret.Action = kvrpcpb.Action_LockNotExistRollback
		} else{ // if timeout, delete lock
			ret.Action = kvrpcpb.Action_TTLExpireRollback
			txn.DeleteLock(key)
		}
		// rollback
		txn.PutWrite(key, lockTs, &mvcc.Write{
			StartTS: lockTs,
			Kind: mvcc.WriteKindRollback,
		})
		txn.DeleteValue(key)
	} else{ // update lock ttl
		ret.LockTtl = lock.Ttl
	}
	
	server.storage.Write(req.Context, txn.Writes())
	return ret, nil
}

// the revert of commit
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	ret := &kvrpcpb.BatchRollbackResponse{}
	ts := req.GetStartVersion()

	// start a txn
	txn, err := server.startMVCCTxn(req.Context, ts)
	if err != nil {
		return ret, err
	}	
	// add latch
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		// check write
		_write, _commitTs, _err := txn.CurrentWrite(key)
		if err != nil{
			return ret, _err
		}
		// write is committed, rollback failed
		if _write != nil && _write.Kind != mvcc.WriteKindRollback && _commitTs > 0 {
			ret.Error = &kvrpcpb.KeyError{
				Abort: "true",
			}
			return ret, nil
		}

		// clean lock
		_lock, _err := txn.GetLock(key)
		if _err != nil{
			return ret, _err
		}
		// clean its lock
		if _lock != nil && _lock.Ts == ts {
			txn.DeleteLock(key)
		} else if _lock == nil && _write != nil { // commit twice, do nothing
			continue
		}

		// rollback
		txn.PutWrite(key, ts, &mvcc.Write{
			StartTS: ts,
			Kind: mvcc.WriteKindRollback,
		})
		txn.DeleteValue(key)
	}

	// write all keys
	err = server.storage.Write(req.Context, txn.Writes())
	return ret, err
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	ret := &kvrpcpb.ResolveLockResponse{}

	ts := req.GetStartVersion()
	// start a txn
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return ret, err
	}
	txn := mvcc.NewMvccTxn(reader, ts)

	// get all locks
	var keys [][]byte
	for iter := reader.IterCF(engine_util.CfLock) ; iter.Valid(); iter.Next() {
		key := iter.Item().Key()
		lock, err := txn.GetLock(key)
		if err != nil {
			return ret, err
		}
		if lock != nil && lock.Ts == ts{ // lock hold by itself
			keys = append(keys, key)
		} 
	}

	if req.CommitVersion == 0 {
		// rollback all locks
		newReq := &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: ts,
			Keys:         keys,
		}
		_,err = server.KvBatchRollback(context.TODO(), newReq)
	} else {
		// commit all locks
		newReq := &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  ts,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		}
		_,err = server.KvCommit(context.TODO(), newReq)
	}
	return ret, err
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
