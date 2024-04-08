package redis

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

type Database struct {
	ethdb.KeyValueStore

	db *redis.Client
}

func New(store ethdb.KeyValueStore) *Database {
	return &Database{
		db: redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
		KeyValueStore: store,
	}
}

func (db *Database) Put(key []byte, value []byte) error {
	cmd := db.db.Set(context.Background(), string(key), value, 0)
	if cmd.Err() != nil {
		log.Error("redis put error", "error", cmd.Err())
		return cmd.Err()
	}
	return db.KeyValueStore.Put(key, value)
}

func (db *Database) Delete(key []byte) error {
	cmd := db.db.Del(context.Background(), string(key))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	return db.KeyValueStore.Delete(key)
}

type batch struct {
	ethdb.Batch

	pipeliner redis.Pipeliner
}

// batch implement ethdb.Batch
func (b *batch) Put(key, value []byte) error {
	if cmd := b.pipeliner.Set(context.Background(), string(key), value, 0); cmd.Err() != nil {
		return cmd.Err()
	}

	return b.Batch.Put(key, value)
}

func (b *batch) Delete(key []byte) error {
	if cmd := b.pipeliner.Del(context.Background(), string(key)); cmd.Err() != nil {
		return cmd.Err()
	}

	return b.Batch.Delete(key)
}

func (b *batch) ValueSize() int {
	return b.Batch.ValueSize()
}

func (b *batch) Write() error {
	if _, err := b.pipeliner.Exec(context.Background()); err != nil {
		return err
	}
	return b.Batch.Write()
}

func (b *batch) Reset() {
	b.pipeliner.Discard()
	b.Batch.Reset()
}

func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	return b.Batch.Replay(w)
}

func (db *Database) NewBatch() ethdb.Batch {
	return &batch{
		pipeliner: db.db.TxPipeline(),
		Batch:     db.KeyValueStore.NewBatch(),
	}
}

func (db *Database) NewBatchWithSize(size int) ethdb.Batch {
	return &batch{
		pipeliner: db.db.TxPipeline(),
		Batch:     db.KeyValueStore.NewBatchWithSize(size),
	}
}
