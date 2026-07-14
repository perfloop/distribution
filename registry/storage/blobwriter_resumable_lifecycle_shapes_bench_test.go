//go:build !noresumabledigest

package storage

import (
	"bytes"
	"context"
	"path"
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
)

func BenchmarkBlobWriterResumeDigestFilesystemLifecycleMissing1(b *testing.B) {
	benchmarkBlobWriterResumeDigestFilesystemLifecycleShape(b, 1, resumableDigestCheckpointMissing)
}

func BenchmarkBlobWriterResumeDigestFilesystemLifecycleMissing128(b *testing.B) {
	benchmarkBlobWriterResumeDigestFilesystemLifecycleShape(b, resumeDigestLifecycleCheckpointCount, resumableDigestCheckpointMissing)
}

func BenchmarkBlobWriterResumeDigestFilesystemLifecycleLegacy128(b *testing.B) {
	benchmarkBlobWriterResumeDigestFilesystemLifecycleShape(b, resumeDigestLifecycleCheckpointCount, resumableDigestCheckpointLegacy)
}

type resumableDigestCheckpointShape int

const (
	resumableDigestCheckpointMissing resumableDigestCheckpointShape = iota
	resumableDigestCheckpointLegacy
)

func benchmarkBlobWriterResumeDigestFilesystemLifecycleShape(b *testing.B, checkpointCount int, shape resumableDigestCheckpointShape) {
	driver := filesystem.New(filesystem.DriverParameters{
		RootDirectory: b.TempDir(),
		MaxThreads:    25,
	})
	ctx, blobs, upload, initial := newResumableDigestUpload(b, driver)
	addResumableDigestCheckpointFiles(b, ctx, driver, upload, checkpointCount)

	writer, ok := upload.(*blobWriter)
	if !ok {
		b.Fatalf("upload writer type = %T, want *blobWriter", upload)
	}
	canonicalCheckpointPath := resumableDigestCheckpointPath(b, writer, writer.written)
	checkpoint, err := driver.GetContent(ctx, canonicalCheckpointPath)
	if err != nil {
		b.Fatalf("read checkpoint: %v", err)
	}
	legacyCheckpointPath := path.Join(path.Dir(canonicalCheckpointPath), "0x11")
	payload := bytes.Repeat([]byte("x"), resumeDigestLifecyclePayloadSize)
	resumedCheckpointPath := resumableDigestCheckpointPath(b, writer, writer.written+int64(len(payload)))
	missingCheckpointPath := resumableDigestCheckpointPath(b, writer, int64(len(payload)))
	uploadID := upload.ID()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		resetResumableDigestUploadShape(b, ctx, driver, writer.path, initial, canonicalCheckpointPath, legacyCheckpointPath, checkpoint, resumedCheckpointPath, missingCheckpointPath, shape)
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
		wantWritten := int64(len(payload))
		if shape == resumableDigestCheckpointLegacy {
			wantWritten += int64(len(initial))
		}
		if resumedWriter.written != wantWritten {
			b.Fatalf("resumed writer digest offset = %d, want %d", resumedWriter.written, wantWritten)
		}
		if err := resumed.Close(); err != nil {
			b.Fatalf("close resumed upload: %v", err)
		}
		b.StopTimer()
	}
}

func resetResumableDigestUploadShape(tb testing.TB, ctx context.Context, driver storagedriver.StorageDriver, uploadPath string, initial []byte, canonicalCheckpointPath, legacyCheckpointPath string, checkpoint []byte, resumedCheckpointPath, missingCheckpointPath string, shape resumableDigestCheckpointShape) {
	tb.Helper()

	if err := driver.PutContent(ctx, uploadPath, initial); err != nil {
		tb.Fatalf("restore upload data: %v", err)
	}
	deleteResumableDigestCheckpoint(tb, ctx, driver, resumedCheckpointPath)
	deleteResumableDigestCheckpoint(tb, ctx, driver, missingCheckpointPath)
	deleteResumableDigestCheckpoint(tb, ctx, driver, canonicalCheckpointPath)
	deleteResumableDigestCheckpoint(tb, ctx, driver, legacyCheckpointPath)

	if shape == resumableDigestCheckpointLegacy {
		if err := driver.PutContent(ctx, legacyCheckpointPath, checkpoint); err != nil {
			tb.Fatalf("restore legacy checkpoint: %v", err)
		}
	}
}

func deleteResumableDigestCheckpoint(tb testing.TB, ctx context.Context, driver storagedriver.StorageDriver, checkpointPath string) {
	tb.Helper()

	if err := driver.Delete(ctx, checkpointPath); err != nil {
		if _, ok := err.(storagedriver.PathNotFoundError); !ok {
			tb.Fatalf("delete checkpoint %q: %v", checkpointPath, err)
		}
	}
}
