//go:build !noresumabledigest

package storage

import (
	"context"
	"testing"

	"github.com/distribution/distribution/v3"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
)

const resumeDigestCheckpointCount = 128

func BenchmarkBlobWriterResumeDigestFilesystem(b *testing.B) {
	driver := filesystem.New(filesystem.DriverParameters{
		RootDirectory: b.TempDir(),
		MaxThreads:    25,
	})
	ctx, blobs, upload, _ := newResumableDigestUpload(b, driver)
	addResumableDigestCheckpointFiles(b, ctx, driver, upload, resumeDigestCheckpointCount)
	uploadID := upload.ID()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		resumed, err := blobs.Resume(ctx, uploadID)
		if err != nil {
			b.Fatalf("resume upload: %v", err)
		}
		b.StartTimer()
		n, err := resumed.ReadFrom(emptyReader{})
		b.StopTimer()
		if err != nil {
			b.Fatalf("read resumed upload: %v", err)
		}
		if n != 0 {
			b.Fatalf("read %d bytes, want 0", n)
		}
	}
}

func addResumableDigestCheckpointFiles(tb testing.TB, ctx context.Context, driver storagedriver.StorageDriver, upload distribution.BlobWriter, count int) {
	tb.Helper()

	writer, ok := upload.(*blobWriter)
	if !ok {
		tb.Fatalf("upload writer type = %T, want *blobWriter", upload)
	}
	for offset := int64(1); offset <= int64(count); offset++ {
		if offset == writer.written {
			continue
		}
		hashStatePath, err := pathFor(uploadHashStatePathSpec{
			name:   writer.blobStore.repository.Named().String(),
			id:     writer.id,
			alg:    writer.digester.Digest().Algorithm(),
			offset: offset,
		})
		if err != nil {
			tb.Fatalf("create hash state path: %v", err)
		}
		if err := driver.PutContent(ctx, hashStatePath, []byte("unused checkpoint")); err != nil {
			tb.Fatalf("store hash state: %v", err)
		}
	}
}
