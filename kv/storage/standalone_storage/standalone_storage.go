package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	config *config.Config
	engine *engine_util.Engines
	txn    *badger.Txn
}

func (s StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	res, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return res, err
}

func (s StandAloneStorage) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)

}
func (s StandAloneStorage) Close() {
	s.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{config: conf}
}

func (s *StandAloneStorage) Start() error {

	kvPath := path.Join(s.config.DBPath, "kv")
	raftPath := path.Join(s.config.DBPath, "raft")

	kvdb := engine_util.CreateDB(kvPath, false)
	raftdb := engine_util.CreateDB(raftPath, true)

	engine := engine_util.NewEngines(kvdb, raftdb, kvPath, raftPath)
	s.engine = engine
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Destroy()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	s.txn = s.engine.Kv.NewTransaction(false)
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engine.Kv, modify.Cf(), modify.Key(), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.engine.Kv, modify.Cf(), modify.Key())
			if err != nil {
				return err
			}
		default:
			return errors.New("not support type")
		}

	}
	return nil
}
