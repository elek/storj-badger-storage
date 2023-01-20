package badger

import (
	"github.com/stretchr/testify/require"
	"io"
	"storj.io/common/testcontext"
	"storj.io/storj/storage"
	"testing"
)

func TestReopen(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	d := ctx.Dir("storage")

	store, err := NewBlobStore(d)
	require.NoError(t, err)

	create, err := store.Create(ctx, storage.BlobRef{
		Namespace: []byte("ns"),
		Key:       []byte("key1"),
	}, -1)
	require.NoError(t, err)

	_, err = create.Write([]byte("test"))
	require.NoError(t, err)

	err = create.Commit(ctx)
	require.NoError(t, err)

	require.NoError(t, store.Close())

	store, err = NewBlobStore(d)
	require.NoError(t, err)

	ns, err := store.ListNamespaces(ctx)
	require.NoError(t, err)

	require.Equal(t, 1, len(ns))
}

func TestMultiWrite(t *testing.T) {
	ctx := testcontext.New(t)
	defer ctx.Cleanup()

	d := ctx.Dir("storage")

	store, err := NewBlobStore(d)
	require.NoError(t, err)

	ref := storage.BlobRef{
		Namespace: []byte("ns"),
		Key:       []byte("key1"),
	}
	create, err := store.Create(ctx, ref, -1)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = create.Write([]byte("test"))
		require.NoError(t, err)
	}

	_, err = create.Write([]byte(""))
	require.NoError(t, err)

	err = create.Commit(ctx)
	require.NoError(t, err)

	reader, err := store.Open(ctx, ref)
	defer reader.Close()
	content, err := rall(reader)
	require.NoError(t, err)
	require.Equal(t, 10*4, len(content))
	require.Equal(t, "testtesttesttesttesttesttesttesttesttest", string(content))
}

func rall(r io.Reader) ([]byte, error) {
	b := make([]byte, 0, 1)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return b, err
		}
	}
}
