// Copyright © 2023 aerth
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package shrinkbolt

import (
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

const (
	oldDbPath = "./testdata/tmpshrink.db"
	newDbPath = "./testdata/tmpshrink.shrunken.db"
)

func TestShrink(t *testing.T) {
	log.SetFlags(log.Lshortfile)
	DangerZone = true
	os.MkdirAll("./testdata", 0755)
	testExpand(t) // need dummy data first
	testDelete(t) // create zombie data
	os.Remove(newDbPath)

	err := ShrinkBoltDatabase(
		oldDbPath,
		newDbPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	stat1, err := os.Stat(oldDbPath)
	if err != nil {
		t.Fatal(err)
	}
	stat2, err := os.Stat(newDbPath)
	if err != nil {
		t.Fatal(err)
	}
	stat1Size := stat1.Size()
	stat2Size := stat2.Size()

	if stat2Size > stat1Size*8/10 { // 20% shrink at least
		t.Fatalf("expected shrink from %d bytes: got %d bytes", stat1Size, stat2Size)
	}

	// TODO: tests to check db2 has required values from db1

}
func testDelete(t *testing.T) {
	db1, err := bbolt.Open(oldDbPath, 0600, &bbolt.Options{
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	err = db1.Update(func(tx *bbolt.Tx) error {
		bu, e := tx.CreateBucketIfNotExists([]byte("names"))
		if e != nil {
			return e
		}
		bu1, e := bu.CreateBucketIfNotExists([]byte("first"))
		if e != nil {
			return e
		}
		e = bu1.DeleteBucket([]byte("junk"))
		if e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

}
func testExpand(t *testing.T) {
	stat, err := os.Stat(oldDbPath)
	if err != nil {
		t.Fatal(err)
	}
	size1 := stat.Size()
	db1, err := bbolt.Open(oldDbPath, 0600, &bbolt.Options{
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {

		stat, err := os.Stat(oldDbPath)
		if err != nil {
			t.Fatal(err)
		}
		size2 := stat.Size()
		log.Println("grew db from", size1, "to", size2, "bytes")
	}()
	defer db1.Close()

	err = db1.Update(func(tx *bbolt.Tx) error {
		bu, e := tx.CreateBucketIfNotExists([]byte("names"))
		if e != nil {
			return e
		}
		bu1, e := bu.CreateBucketIfNotExists([]byte("first"))
		if e != nil {
			return e
		}
		bu2, e := bu1.CreateBucketIfNotExists([]byte("american"))
		if e != nil {
			return e
		}
		for i, v := range []string{"john", "joe", "bob"} {
			e = bu2.Put([]byte(v), []byte{byte(i)})
		}
		if e != nil {
			return e
		}
		junkBucket, e := bu1.CreateBucketIfNotExists([]byte("junk"))
		if e != nil {
			return e
		}
		for i := 0; i < 10000; i++ {
			fmt.Printf(".")
			var buf = make([]byte, 4096)
			rand.Read(buf)
			n := []byte(fmt.Sprintf("%d", i))
			e = junkBucket.Put(n, []byte(fmt.Sprintf("%02x", buf)))
			if e != nil {
				return e
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
