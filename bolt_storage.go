/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
*/

package boltstorage

import (
	"bytes"
	"errors"
	"github.com/boltdb/bolt"
	"go.arpabet.com/storage"
	"io"
	"os"
)

type boltStorage struct {
	name   string
	db     *bolt.DB

	dataDir string
	dataFilePerm os.FileMode
	options []Option
}

func New(name string, dataDir string, dataFilePerm os.FileMode, options... Option) (storage.ManagedStorage, error) {

	if name == "" {
		return nil, errors.New("empty bean name")
	}

	db, err := OpenDatabase(dataDir, dataFilePerm, options...)
	if err != nil {
		return nil, err
	}

	return &boltStorage {
		name: name,
		db: db,
		dataDir: dataDir,
		dataFilePerm: dataFilePerm,
		options: options,
	}, nil
}

func FromDB(name string, db *bolt.DB) storage.ManagedStorage {
	return &boltStorage {name: name, db: db}
}

func (t* boltStorage) BeanName() string {
	return t.name
}

func (t* boltStorage) Destroy() error {
	return t.db.Close()
}

func (t* boltStorage) Get() *storage.GetOperation {
	return &storage.GetOperation{Storage: t}
}

func (t* boltStorage) Set() *storage.SetOperation {
	return &storage.SetOperation{Storage: t}
}

func (t *boltStorage) Increment() *storage.IncrementOperation {
	return &storage.IncrementOperation{Storage: t, Initial: 0, Delta: 1}
}

func (t* boltStorage) CompareAndSet() *storage.CompareAndSetOperation {
	return &storage.CompareAndSetOperation{Storage: t}
}

func (t* boltStorage) Remove() *storage.RemoveOperation {
	return &storage.RemoveOperation{Storage: t}
}

func (t* boltStorage) Enumerate() *storage.EnumerateOperation {
	return &storage.EnumerateOperation{Storage: t}
}

func (t* boltStorage) GetRaw(bucket, key []byte, ttlPtr *int, versionPtr *int64, required bool) ([]byte, error) {
	return t.getImpl(bucket, key, required)
}

func (t* boltStorage) SetRaw(bucket, key, value []byte, ttlSeconds int) error {

	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	return t.db.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		return b.Put(key, value)

	})

}

func (t *boltStorage) DoInTransaction(bucket, key []byte, cb func(entry *storage.RawEntry) bool) error {

	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	return t.db.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		re := storage.RawEntry{
			Key:     key,
			Value:   b.Get(key),
			Ttl:     0,
			Version: 0,
		}

		if !cb(&re) {
			return ErrCanceled
		}

		return b.Put(key, re.Value)
	})
}

func (t* boltStorage) CompareAndSetRaw(bucket, key, value []byte, ttlSeconds int, version int64) (bool, error) {
	return true, t.SetRaw(bucket, key, value, ttlSeconds)
}

func (t* boltStorage) RemoveRaw(bucket, key []byte) error {

	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	return t.db.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}

		return b.Delete(key)
	})

}

func (t* boltStorage) getImpl(bucket, key []byte, required bool) ([]byte, error) {

	var val []byte

	err := t.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}

		val = b.Get(key)
		return nil
	})

	if err != nil {
		return nil, err
	}

	if val == nil && required {
		return nil, os.ErrNotExist
	}

	return val, nil
}

func (t* boltStorage) EnumerateRaw(bucket, seek []byte, batchSize int, onlyKeys bool, cb func(entry *storage.RawEntry) bool) error {

	// for API compatibility with other storage impls (PnP)
	if !bytes.HasPrefix(seek, bucket) {
		return ErrInvalidSeek
	}

	seek = seek[len(bucket):]

	return t.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}

		cur := b.Cursor()
		k, v := cur.Seek(seek)  // within bucket
		if k != nil {
			re := storage.RawEntry{
				Key:     k,
				Value:   v,
				Ttl:     0,
				Version: 0,
			}
			if !cb(&re) {
				return nil
			}
		}

		for k, v := cur.Seek(seek); k != nil; k, v = cur.Next() {
			re := storage.RawEntry{
				Key:     k,
				Value:   v,
				Ttl:     0,
				Version: 0,
			}
			if !cb(&re) {
				return nil
			}
		}

		return nil

	})

}

func (t* boltStorage) FetchKeysRaw(bucket []byte, batchSize int) ([][]byte, error) {

	var keys [][]byte

	t.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		
		return b.ForEach(func(k, v []byte) error {
			keys = append(keys, k)
			return nil
		})

	})

	return keys, nil
}

func (t* boltStorage) Compact(discardRatio float64) error {
	// bolt does not support compaction
	return nil
}

func (t* boltStorage) Backup(w io.Writer, since uint64) (uint64, error) {

	var txId int

	err := t.db.View(func(tx *bolt.Tx) error {
		txId = tx.ID()
		_, err := tx.WriteTo(w)
		return err
	})

	return uint64(txId), err

}

func (t* boltStorage) Restore(src io.Reader) error {

	dbPath := t.db.Path()
	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	err := t.db.Close()
	if err != nil {
		return err
	}

	dst, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, t.dataFilePerm)
	if err != nil {
		return err
	}

	_, err = io.Copy(dst, src)
	if err != nil {
		return err
	}

	opts := &bolt.Options{}
	for _, opt := range t.options {
		opt.apply(opts)
	}
	opts.ReadOnly = false

	t.db, err = bolt.Open(dbPath, t.dataFilePerm, opts)
	return err
}

func (t* boltStorage) DropAll() error {

	dbPath := t.db.Path()
	if t.db.IsReadOnly() {
		return ErrDatabaseReadOnly
	}

	err := t.db.Close()
	if err != nil {
		return err
	}

	err = os.Remove(dbPath)
	if err != nil {
		return err
	}

	opts := &bolt.Options{}
	for _, opt := range t.options {
		opt.apply(opts)
	}

	t.db, err = bolt.Open(dbPath, t.dataFilePerm, opts)
	return err
}

func (t* boltStorage) DropWithPrefix(bucket []byte) error {

	return t.db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		
		return b.ForEach(func(k, v []byte) error {
			return b.Delete(k)
		})

	})

}

func (t* boltStorage) Instance() interface{} {
	return t.db
}