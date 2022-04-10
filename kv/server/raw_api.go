package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	bytes, err := reader.GetCF(req.GetCf(), req.GetKey())
	if bytes == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}
	reader.Close()
	resp := &kvrpcpb.RawGetResponse{Value: bytes}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{Data: storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}}
	return &kvrpcpb.RawPutResponse{}, server.storage.Write(req.GetContext(), []storage.Modify{modify})

}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{Data: storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}}
	return &kvrpcpb.RawDeleteResponse{}, server.storage.Write(req.GetContext(), []storage.Modify{modify})
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	kvpairs := make([]*kvrpcpb.KvPair, 0)
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	iterator := reader.IterCF(req.GetCf())
	iterator.Seek(req.StartKey)
	for i := uint32(0); i < req.Limit; i++ {
		if iterator.Valid() {
			item := iterator.Item()
			value, _ := item.Value()
			kvpairs = append(kvpairs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
			iterator.Next()
		}
	}
	iterator.Close()
	reader.Close()
	return &kvrpcpb.RawScanResponse{Kvs: kvpairs}, nil
}
