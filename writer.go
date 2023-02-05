package badger

import (
	"context"
	badger "github.com/dgraph-io/badger/v3"
	"github.com/zeebo/errs"
	"io"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
)

type writer struct {
	offset int
	length int
	buffer []byte
	ref    storage.BlobRef
	db     *badger.DB
}

func NewWriter(db *badger.DB, ref storage.BlobRef) *writer {
	return &writer{
		db:     db,
		ref:    ref,
		buffer: make([]byte, 5000000),
	}
}
func (w *writer) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		panic("implement me")
	}
	w.offset = int(offset)
	if w.offset > w.length {
		w.length = w.offset
	}
	return int64(w.offset), nil
}

func (w *writer) Cancel(ctx context.Context) error {
	w.buffer = nil
	return nil
}

func (w *writer) Commit(ctx context.Context) error {
	if w.buffer == nil {
		return errs.New("Already committed")
	}
	err := w.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key(w.ref), w.buffer[:w.offset])
	})
	w.buffer = nil
	return err

}

func key(ref storage.BlobRef) []byte {
	return append(append(blobPrefix, ref.Namespace...), ref.Key...)
}

func trashKey(ref storage.BlobRef) []byte {
	return append(append(trashPrefix, ref.Namespace...), ref.Key...)
}

func (w *writer) Size() (int64, error) {
	return int64(w.offset), nil
}

func (w *writer) StorageFormatVersion() storage.FormatVersion {
	return filestore.FormatV1
}

func (w *writer) Write(p []byte) (n int, err error) {
	i := copy(w.buffer[w.offset:len(p)+w.offset], p)
	w.offset += i
	if w.offset > w.length {
		w.length = w.offset
	}
	return i, nil
}
