package badger

import (
	"context"
	"io/fs"
	"os"
	"storj.io/storj/storage"
	"storj.io/storj/storage/filestore"
	"time"
)

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
