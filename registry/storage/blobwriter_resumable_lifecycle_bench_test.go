//go:build !noresumabledigest

package storage

import (
	"bytes"
	"context"
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
)

const (
	resumeDigestLifecycleCheckpointCount = 128
	resumeDigestLifecyclePayloadSize     = 32 << 10
)

// BenchmarkBlobWriterResumeDigestFilesystemLifecycle measures the storage
// lifecycle performed by one resumed upload chunk. Setup restores the same
// pre-existing upload outside the measured region; the timed operation resumes,
// appends a nonempty chunk, and closes the writer as the request path does.
func BenchmarkBlobWriterResumeDigestFilesystemLifecycle(b *testing.B) {
	driver := filesystem.New(filesystem.DriverParameters{
		RootDirectory: b.TempDir(),
		MaxThreads:    25,
	})
	ctx, blobs, upload, initial := newResumableDigestUpload(b, driver)
	addResumableDigestCheckpointFiles(b, ctx, driver, upload, resumeDigestLifecycleCheckpointCount)

	writer, ok := upload.(*blobWriter)
	if !ok {
		b.Fatalf("upload writer type = %T, want *blobWriter", upload)
	}
	checkpointPath := resumableDigestCheckpointPath(b, writer, writer.written)
	checkpoint, err := driver.GetContent(ctx, checkpointPath)
	if err != nil {
		b.Fatalf("read checkpoint: %v", err)
	}
	nextCheckpointPath := resumableDigestCheckpointPath(b, writer, writer.written+resumeDigestLifecyclePayloadSize)
	payload := bytes.Repeat([]byte("x"), resumeDigestLifecyclePayloadSize)
	uploadID := upload.ID()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		resetResumableDigestUpload(b, ctx, driver, writer.path, initial, checkpointPath, checkpoint, nextCheckpointPath)
		b.StartTimer()

		resumed, err := blobs.Resume(ctx, uploadID)
		if err != nil {
			b.Fatalf("resume upload: %v", err)
		}
		n, err := resumed.ReadFrom(bytes.NewReader(payload))
		if err != nil {
			b.Fatalf("append resumed upload: %v", err)
		}
		if n != int64(len(payload)) {
			b.Fatalf("appended %d bytes, want %d", n, len(payload))
		}
		resumedWriter, ok := resumed.(*blobWriter)
		if !ok {
			b.Fatalf("resumed writer type = %T, want *blobWriter", resumed)
		}
		if resumedWriter.written != int64(len(initial)+len(payload)) {
			b.Fatalf("resumed writer digest offset = %d, want %d", resumedWriter.written, len(initial)+len(payload))
		}
		if err := resumed.Close(); err != nil {
			b.Fatalf("close resumed upload: %v", err)
		}
		b.StopTimer()
	}
}

func resumableDigestCheckpointPath(tb testing.TB, writer *blobWriter, offset int64) string {
	tb.Helper()

	checkpointPath, err := pathFor(uploadHashStatePathSpec{
		name:   writer.blobStore.repository.Named().String(),
		id:     writer.id,
		alg:    writer.digester.Digest().Algorithm(),
		offset: offset,
	})
	if err != nil {
		tb.Fatalf("create hash state path: %v", err)
	}
	return checkpointPath
}

func resetResumableDigestUpload(tb testing.TB, ctx context.Context, driver storagedriver.StorageDriver, uploadPath string, initial []byte, checkpointPath string, checkpoint []byte, nextCheckpointPath string) {
	tb.Helper()

	if err := driver.PutContent(ctx, uploadPath, initial); err != nil {
		tb.Fatalf("restore upload data: %v", err)
	}
	if err := driver.PutContent(ctx, checkpointPath, checkpoint); err != nil {
		tb.Fatalf("restore checkpoint: %v", err)
	}
	if err := driver.Delete(ctx, nextCheckpointPath); err != nil {
		if _, ok := err.(storagedriver.PathNotFoundError); !ok {
			tb.Fatalf("delete next checkpoint: %v", err)
		}
	}
}
