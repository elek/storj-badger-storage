package badger

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/zeebo/errs"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
	"time"
)

var namespacePrefix = []byte("nmspc")
var blobPrefix = []byte("blobs")
var trashPrefix = []byte("trash")

var verificationFileName = "storage-badger-verification"

type BlobStore struct {
	db         *badger.DB
	namespaces [][]byte
	dir        string
}

var _ storage.Blobs = &BlobStore{}

func NewBlobStore(dir string) (*BlobStore, error) {
	db, err := badger.Open(badger.DefaultOptions(dir))
	if err != nil {
		return nil, errs.Wrap(err)
	}
	namespaces := make([][]byte, 0)
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		for it.Seek(namespacePrefix); it.ValidForPrefix(namespacePrefix); it.Next() {
			namespaces = append(namespaces, it.Item().Key()[len(namespacePrefix):])
		}
		it.Close()
		return nil
	})
	return &BlobStore{
		dir:        dir,
		db:         db,
		namespaces: namespaces,
	}, nil
}
func (b *BlobStore) Create(ctx context.Context, ref storage.BlobRef, size int64) (storage.BlobWriter, error) {
	err := b.ensureNamespace(ref)
	return NewWriter(b.db, ref), err
}

func (b *BlobStore) Open(ctx context.Context, ref storage.BlobRef) (storage.BlobReader, error) {
	return NewReader(b.db, ref)
}

func (b *BlobStore) OpenWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) (storage.BlobReader, error) {
	if formatVer != filestore.FormatV1 {
		return nil, errs.New("Unsupported format")
	}
	return b.Open(ctx, ref)
}

func (b *BlobStore) Delete(ctx context.Context, ref storage.BlobRef) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key(ref))
	})
}

func (b *BlobStore) DeleteWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) error {
	if formatVer != filestore.FormatV1 {
		return errs.New("Unsupported format")
	}
	return b.Delete(ctx, ref)
}

func (b *BlobStore) DeleteNamespace(ctx context.Context, ref []byte) (err error) {
	ns := append(namespacePrefix, ref...)
	return b.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ns); it.ValidForPrefix(ns); it.Next() {
			err = txn.Delete(it.Item().Key())
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BlobStore) Trash(ctx context.Context, ref storage.BlobRef) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return b.move(txn, key(ref), trashKey(ref))
	})
}

func (b *BlobStore) move(txn *badger.Txn, from []byte, to []byte) error {
	item, err := txn.Get(from)
	if err != nil {
		return err
	}
	err = item.Value(func(val []byte) error {
		return txn.Set(to, val)
	})
	if err != nil {
		return err
	}
	return txn.Delete(from)
}

func (b *BlobStore) RestoreTrash(ctx context.Context, namespace []byte) ([][]byte, error) {
	var keys [][]byte
	err := b.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(trashPrefix); it.ValidForPrefix(trashPrefix); it.Next() {
			key := it.Item().Key()
			keys = append(keys, key)
			origKey := append(blobPrefix, key[len(trashPrefix):]...)
			err := b.move(txn, key, origKey)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return keys, err
}

func (b *BlobStore) EmptyTrash(ctx context.Context, namespace []byte, trashedBefore time.Time) (int64, [][]byte, error) {
	var keys [][]byte
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(trashPrefix); it.ValidForPrefix(trashPrefix); it.Next() {
			key := it.Item().Key()
			keys = append(keys, key)
			err := txn.Delete(key)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return 0, keys, err
}

func (b *BlobStore) Stat(ctx context.Context, ref storage.BlobRef) (storage.BlobInfo, error) {
	var size int64
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key(ref))
		if err != nil {
			return errs.Wrap(err)
		}
		err = item.Value(func(val []byte) error {
			size = int64(len(val))
			return nil
		})
		return err
	})
	return BlobInfo{
		ref:  ref,
		size: size,
	}, err
}

func (b *BlobStore) StatWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) (storage.BlobInfo, error) {
	if formatVer != filestore.FormatV1 {
		return nil, errs.New("Unsupported format")
	}
	return b.Stat(ctx, ref)
}

func (b *BlobStore) FreeSpace(ctx context.Context) (int64, error) {
	info, err := diskInfoFromPath(b.dir)
	return info.AvailableSpace, err
}

func (b *BlobStore) CheckWritability(ctx context.Context) error {
	return nil
}

func (b *BlobStore) SpaceUsedForTrash(ctx context.Context) (int64, error) {
	s := int64(0)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(trashPrefix); it.ValidForPrefix(trashPrefix); it.Next() {
			item := it.Item()
			s += item.ValueSize()
		}
		return nil
	})
	return s, err
}

func (b *BlobStore) SpaceUsedForBlobs(ctx context.Context) (int64, error) {
	s := int64(0)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(namespacePrefix); it.ValidForPrefix(namespacePrefix); it.Next() {
			item := it.Item()
			s += item.ValueSize()
		}
		return nil
	})
	return s, err
}

func (b *BlobStore) SpaceUsedForBlobsInNamespace(ctx context.Context, namespace []byte) (int64, error) {
	s := int64(0)
	ns := append(namespacePrefix, namespace...)
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ns); it.ValidForPrefix(ns); it.Next() {
			item := it.Item()
			s += item.ValueSize()
		}
		return nil
	})
	return s, err
}

func (b *BlobStore) ListNamespaces(ctx context.Context) ([][]byte, error) {
	return b.namespaces, nil
}

func (b *BlobStore) WalkNamespace(ctx context.Context, namespace []byte, walkFunc func(storage.BlobInfo) error) error {
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(ns(namespace)); it.ValidForPrefix(ns(namespace)); it.Next() {
			item := it.Item()
			err := walkFunc(BlobInfo{
				ref: storage.BlobRef{
					Namespace: namespace,
					Key:       item.Key()[len(ns(namespace)):],
				},
				name: string(item.Key()[len(ns(namespace)):]),
				// This is just estimation!!!!
				size: item.ValueSize(),
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func ns(namespace []byte) []byte {
	return append(blobPrefix, namespace...)
}

func (b *BlobStore) CreateVerificationFile(ctx context.Context, id storj.NodeID) error {
	f, err := os.Create(filepath.Join(b.dir, verificationFileName))
	if err != nil {
		return err
	}
	defer func() {
		err = errs.Combine(err, f.Close())
	}()
	_, err = f.Write(id.Bytes())
	return err
}

func (b *BlobStore) VerifyStorageDir(ctx context.Context, id storj.NodeID) error {
	content, err := os.ReadFile(filepath.Join(b.dir, verificationFileName))
	if err != nil {
		return err
	}

	if !bytes.Equal(content, id.Bytes()) {
		verifyID, err := storj.NodeIDFromBytes(content)
		if err != nil {
			return errs.New("content of file is not a valid node ID: %x", content)
		}
		return errs.New("node ID in file (%s) does not match running node's ID (%s)", verifyID, id.String())
	}
	return nil
}

func (b *BlobStore) Close() error {
	return b.db.Close()
}

func (b *BlobStore) ensureNamespace(ref storage.BlobRef) error {
	for _, ns := range b.namespaces {
		if bytesEq(ns, ref.Namespace) {
			return nil
		}
	}
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(append(namespacePrefix, ref.Namespace...), []byte{1})
	})
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	b.namespaces = append(b.namespaces, ref.Namespace)
	return nil
}

func bytesEq(ns []byte, namespace []byte) bool {
	if len(ns) != len(namespace) {
		return false
	}
	for i := 0; i < len(ns); i++ {
		if ns[i] != namespace[i] {
			return false
		}
	}
	return true
}

func diskInfoFromPath(path string) (info DiskInfo, err error) {
	var stat unix.Statfs_t
	err = unix.Statfs(path, &stat)
	if err != nil {
		return DiskInfo{"", -1}, err
	}

	// the Bsize size depends on the OS and unconvert gives a false-positive
	availableSpace := int64(stat.Bavail) * int64(stat.Bsize) //nolint: unconvert
	filesystemID := fmt.Sprintf("%08x%08x", stat.Fsid.Val[0], stat.Fsid.Val[1])

	return DiskInfo{filesystemID, availableSpace}, nil
}

// DiskInfo contains statistics about this dir.
type DiskInfo struct {
	ID             string
	AvailableSpace int64
}
