package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	dbPath string
	bindAddr string
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	s := &StandAloneStorage{
		dbPath: conf.DBPath,
		bindAddr: conf.StoreAddr,
	}
	if s.dbPath == "" {
		s.dbPath = "/tmp/badger"
	}
	if s.bindAddr == "" {
		s.bindAddr = "0.0.0.0:8888"
	}
	return s
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.dbPath
	opts.ValueDir = s.dbPath
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	// bind
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return StandAloneStorageReader{
		db: s.db,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			return engine_util.PutCF(s.db, m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			return engine_util.DeleteCF(s.db, m.Cf(), m.Key())
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	db *badger.DB
}

func (s StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	bs, err := engine_util.GetCF(s.db, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return bs, err
}

func (s StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.db.NewTransaction(false))
}

func (s StandAloneStorageReader) Close() {

}
