package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	bs, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	rsp := &kvrpcpb.RawGetResponse{
		Error:    "",
		Value:    bs,
		NotFound: bs == nil,
	}
	return rsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	err := server.storage.Write(req.Context, []storage.Modify{
		{Data: put},
	})
	rsp := &kvrpcpb.RawPutResponse{
		Error: fmt.Sprintf("%v", err),
	}
	return rsp, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	err := server.storage.Write(req.Context, []storage.Modify{
		{Data: delete},
	})
	rsp := &kvrpcpb.RawDeleteResponse{
		Error: fmt.Sprintf("%v", err),
	}
	return rsp, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	rsp := &kvrpcpb.RawScanResponse{
		Error:                "",
		Kvs:                  []*kvrpcpb.KvPair{},
	}
	for i:=uint32(0); i<req.Limit && iter.Valid(); i++ {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			rsp.Error = fmt.Sprintf("%v", err)
			return rsp, err
		}
		pair := kvrpcpb.KvPair{
			Error:                nil,
			Key:                  item.Key(),
			Value:                value,
		}
		rsp.Kvs = append(rsp.Kvs, &pair)
		iter.Next()
	}
	return rsp, nil
}
