// Copyright © 2023 aerth
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// shrinkbolt package shrinks bolt database files
package shrinkbolt

import (
	"fmt"
	"log"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

var Options = &bbolt.Options{
	Timeout: time.Second,
}
var DangerZone = false

func ShrinkBoltDatabase(oldDbPath string, newDbPath string) error {
	if oldDbPath == "" {
		return fmt.Errorf("missing current db path")
	}
	if newDbPath == "" {
		return fmt.Errorf("missing new db path (should not exist)")
	}
	if oldDbPath == newDbPath {
		return fmt.Errorf("paths should not equal")
	}
	_, err := os.Stat(newDbPath)
	if err == nil {
		return fmt.Errorf("new db exists")
	}

	// open dbs
	olddb, err := bbolt.Open(oldDbPath, 0600, Options)
	if err != nil {
		return err
	}
	defer func() {
		if err := olddb.Close(); err != nil {
			log.Println("err closing old db:", err)
		}
	}()
	newdb, err := bbolt.Open(newDbPath, 0600, Options)
	if err != nil {
		return err
	}
	defer func() {
		if err := newdb.Close(); err != nil {
			log.Println("err closing old db:", err)
		}
	}()

	// shrink
	up := Upgrader{
		Old: olddb,
		New: newdb,
	}
	err = up.Shrink()
	if err != nil {
		return err
	}
	return nil
}

type Upgrader struct {
	Old *bbolt.DB
	New *bbolt.DB
}

func fmtstr(k [][]byte) string {
	var s string
	for i := range k {
		s += fmt.Sprintf("[%s]", k[i])
	}
	return s

}

// writeCopy creates nested buckets in new db, and puts data in the final k bucket
func (u Upgrader) WriteCopy(v []byte, k ...[]byte) error {
	if len(v) == 0 {
		return nil
	}
	return u.New.Update(func(tx *bbolt.Tx) error {
		bu, err := GetNestedBucket(tx, k, true)
		if err != nil {
			return err
		}
		return bu.Put(k[len(k)-1], v)
	})
}

// getNestedBucket presents the bucket at bucketPath. createIfNotExist depends on tx being a write-transaction
func GetNestedBucket(tx *bbolt.Tx, bucketPath [][]byte, createIfNotExist bool) (*bbolt.Bucket, error) {
	k := bucketPath
	if len(k) < 1 {
		return nil, fmt.Errorf("need >1 keys")
	}
	// wrapper to conform to func() (bucket,error) type
	bucketFn := func(k []byte) (*bbolt.Bucket, error) {
		return tx.Bucket(k), nil
	}
	if createIfNotExist {
		bucketFn = tx.CreateBucketIfNotExists
	}
	// entry
	bu, err := bucketFn(k[0])
	if err != nil {
		return nil, err
	}
	for i := 1; i <= len(k)-1; i++ {
		innerfn := func(k []byte) (*bbolt.Bucket, error) {
			return bu.Bucket(k), nil
		}
		if createIfNotExist {
			innerfn = bu.CreateBucketIfNotExists
		}
		bu, err = innerfn(k[i])
		if err != nil {
			return nil, err
		}
	}
	return bu, nil

}

// var depth = 0

func (u Upgrader) ReadyWalker(baseBucket [][]byte, tx *bbolt.Tx) error {
	// depth++
	// defer func() {
	// 	depth--
	// }()

	// fresh array
	var currentPath [][]byte = make([][]byte, len(baseBucket))
	copy(currentPath, baseBucket)
	// log.Println("init", depth, fmtstr(baseBucket), "=", fmtstr(currentPath))
	oldbucket, err := GetNestedBucket(tx, currentPath, false)
	if err != nil {
		// panic(err)
		return err
	}
	if oldbucket == nil {
		return fmt.Errorf("bucket not found: %v", fmtstr(currentPath))
	}
	err = oldbucket.ForEach(func(k, v []byte) error {
		// log.Println(depth, "walking", fmtstr(baseBucket), string(k))

		cpath := append(baseBucket, k)
		// is nested bucket
		if v == nil && oldbucket.Bucket(k) != nil {
			// log.Println(depth, "bucket", fmtstr(currentPath), "->", fmtstr(cpath))
			return u.ReadyWalker(cpath, tx)
		}
		if len(v) == 0 {
			println("caught zero length", fmtstr(currentPath))
			return nil
		}
		// log.Println(depth, "data", fmtstr(cpath), string(v))
		err := u.WriteCopy(v, cpath...)
		return err
	})
	if err != nil {
		return err
	}
	return nil
}
func (u Upgrader) Shrink() error {
	if !DangerZone {
		return fmt.Errorf("this is experimental software: please set 'shrinkbolt.DangerZone = true' somewhere before calling Shrink()")
	}
	return u.Old.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, bucket *bbolt.Bucket) error {
			return u.ReadyWalker([][]byte{name}, tx)

		})
	})
}
