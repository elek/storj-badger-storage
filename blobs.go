package badger

import (
	"bytes"
	"context"
	"github.com/dgraph-io/badger/v3"
	"github.com/zeebo/errs"
	"io/fs"
	"os"
	"path/filepath"
	"storj.io/common/storj"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
	"time"
)

var namespacePrefix = []byte("namespace")
var verificationFileName = "storage-badger-verification"

type BlobStore struct {
	db         *badger.DB
	namespaces [][]byte
	dir        string
}

var _ storage.Blobs = &BlobStore{}

type BlobInfo struct {
	ref  storage.BlobRef
	size int64
	name string
}

func (i BlobInfo) BlobRef() storage.BlobRef {
	return i.ref
}

func (i BlobInfo) StorageFormatVersion() storage.FormatVersion {
	return filestore.FormatV1
}

func (i BlobInfo) FullPath(ctx context.Context) (string, error) {
	return "", nil
}

func (i BlobInfo) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo{
		name: i.name,
		size: i.size,
	}, nil
}

var _ storage.BlobInfo = BlobInfo{}

type FileInfo struct {
	name string
	size int64
}

func (f FileInfo) Name() string {
	return f.name
}

func (f FileInfo) Size() int64 {
	return f.size
}

func (f FileInfo) Mode() fs.FileMode {
	//TODO implement me
	panic("implement me")
}

func (f FileInfo) ModTime() time.Time {
	return time.Now()
}

func (f FileInfo) IsDir() bool {
	return false
}

func (f FileInfo) Sys() any {
	return ""
}

var _ os.FileInfo = &FileInfo{}

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
	return nil
}

func (b *BlobStore) Trash(ctx context.Context, ref storage.BlobRef) error {
	return nil
}

func (b *BlobStore) RestoreTrash(ctx context.Context, namespace []byte) ([][]byte, error) {
	return [][]byte{}, nil
}

func (b *BlobStore) EmptyTrash(ctx context.Context, namespace []byte, trashedBefore time.Time) (int64, [][]byte, error) {
	return 0, [][]byte{}, nil
}

func (b *BlobStore) Stat(ctx context.Context, ref storage.BlobRef) (storage.BlobInfo, error) {
	return nil, nil
}

func (b *BlobStore) StatWithStorageFormat(ctx context.Context, ref storage.BlobRef, formatVer storage.FormatVersion) (storage.BlobInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) FreeSpace(ctx context.Context) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) CheckWritability(ctx context.Context) error {
	return nil
}

func (b *BlobStore) SpaceUsedForTrash(ctx context.Context) (int64, error) {
	return 0, nil
}

func (b *BlobStore) SpaceUsedForBlobs(ctx context.Context) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) SpaceUsedForBlobsInNamespace(ctx context.Context, namespace []byte) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobStore) ListNamespaces(ctx context.Context) ([][]byte, error) {
	return b.namespaces, nil
}

func (b *BlobStore) WalkNamespace(ctx context.Context, namespace []byte, walkFunc func(storage.BlobInfo) error) error {
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(namespace); it.ValidForPrefix(namespace); it.Next() {
			item := it.Item()
			err := walkFunc(BlobInfo{
				ref: storage.BlobRef{
					Namespace: namespace,
					Key:       item.Key()[len(namespace):],
				},
				name: string(item.Key()[len(namespace):]),
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
