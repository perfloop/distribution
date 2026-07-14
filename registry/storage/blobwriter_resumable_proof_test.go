//go:build !noresumabledigest

package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/distribution/distribution/v3"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestBlobWriterResumeDigest(t *testing.T) {
	operations := []struct {
		name  string
		write func(distribution.BlobWriter, []byte) (int64, error)
	}{
		{
			name: "write",
			write: func(writer distribution.BlobWriter, payload []byte) (int64, error) {
				n, err := writer.Write(payload)
				return int64(n), err
			},
		},
		{
			name: "read-from",
			write: func(writer distribution.BlobWriter, payload []byte) (int64, error) {
				return writer.ReadFrom(bytes.NewReader(payload))
			},
		},
	}

	checkpointStates := []struct {
		name             string
		removeCheckpoint bool
	}{
		{name: "exact-checkpoint"},
		{name: "missing-checkpoint", removeCheckpoint: true},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			for _, checkpointState := range checkpointStates {
				t.Run(checkpointState.name, func(t *testing.T) {
					driver := inmemory.New()
					ctx, blobs, upload, initial := newResumableDigestUpload(t, driver)
					if checkpointState.removeCheckpoint {
						removeResumableDigestCheckpoint(t, ctx, driver, upload)
					}

					resumed, err := blobs.Resume(ctx, upload.ID())
					if err != nil {
						t.Fatalf("resume upload: %v", err)
					}

					suffix := []byte("next chunk")
					n, err := operation.write(resumed, suffix)
					if err != nil {
						t.Fatalf("append upload: %v", err)
					}
					if n != int64(len(suffix)) {
						t.Fatalf("appended %d bytes, want %d", n, len(suffix))
					}

					expected := append(append([]byte(nil), initial...), suffix...)
					wantDigest := digest.FromBytes(expected)
					desc, err := resumed.Commit(ctx, v1.Descriptor{Digest: wantDigest})
					if err != nil {
						t.Fatalf("commit upload: %v", err)
					}
					if desc.Digest != wantDigest {
						t.Fatalf("committed digest = %s, want %s", desc.Digest, wantDigest)
					}
					if desc.Size != int64(len(expected)) {
						t.Fatalf("committed size = %d, want %d", desc.Size, len(expected))
					}
				})
			}
		})
	}
}

func BenchmarkBlobWriterResumeDigestStorageCalls(b *testing.B) {
	driver := &resumeDigestCountingDriver{StorageDriver: inmemory.New()}
	ctx, blobs, upload, _ := newResumableDigestUpload(b, driver)
	uploadID := upload.ID()

	var storageCalls int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		resumed, err := blobs.Resume(ctx, uploadID)
		if err != nil {
			b.Fatalf("resume upload: %v", err)
		}
		driver.resetCounts()
		b.StartTimer()
		n, err := resumed.ReadFrom(emptyReader{})
		b.StopTimer()
		if err != nil {
			b.Fatalf("read resumed upload: %v", err)
		}
		if n != 0 {
			b.Fatalf("read %d bytes, want 0", n)
		}
		storageCalls += driver.storageCalls()
	}
	b.ReportMetric(float64(storageCalls)/float64(b.N), "storage_calls/op")
}

func newResumableDigestUpload(tb testing.TB, driver storagedriver.StorageDriver) (context.Context, distribution.BlobStore, distribution.BlobWriter, []byte) {
	tb.Helper()

	ctx := context.Background()
	imageName, err := reference.WithName("resume-digest/test")
	if err != nil {
		tb.Fatalf("create repository name: %v", err)
	}

	registry, err := NewRegistry(ctx, driver)
	if err != nil {
		tb.Fatalf("create registry: %v", err)
	}
	repository, err := registry.Repository(ctx, imageName)
	if err != nil {
		tb.Fatalf("create repository: %v", err)
	}
	blobs := repository.Blobs(ctx)

	initial := []byte("stored checkpoint")
	upload, err := blobs.Create(ctx)
	if err != nil {
		tb.Fatalf("create upload: %v", err)
	}
	if n, err := upload.Write(initial); err != nil {
		tb.Fatalf("write initial upload: %v", err)
	} else if n != len(initial) {
		tb.Fatalf("wrote %d bytes, want %d", n, len(initial))
	}
	if err := upload.Close(); err != nil {
		tb.Fatalf("close initial upload: %v", err)
	}

	return ctx, blobs, upload, initial
}

func removeResumableDigestCheckpoint(tb testing.TB, ctx context.Context, driver storagedriver.StorageDriver, upload distribution.BlobWriter) {
	tb.Helper()

	writer, ok := upload.(*blobWriter)
	if !ok {
		tb.Fatalf("upload writer type = %T, want *blobWriter", upload)
	}
	hashStatePath, err := pathFor(uploadHashStatePathSpec{
		name:   writer.blobStore.repository.Named().String(),
		id:     writer.id,
		alg:    writer.digester.Digest().Algorithm(),
		offset: writer.written,
	})
	if err != nil {
		tb.Fatalf("create hash state path: %v", err)
	}
	if err := driver.Delete(ctx, hashStatePath); err != nil {
		tb.Fatalf("delete hash state: %v", err)
	}
}

type resumeDigestCountingDriver struct {
	storagedriver.StorageDriver
	getContentCalls int
	listCalls       int
}

func (d *resumeDigestCountingDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	d.getContentCalls++
	return d.StorageDriver.GetContent(ctx, path)
}

func (d *resumeDigestCountingDriver) List(ctx context.Context, path string) ([]string, error) {
	d.listCalls++
	return d.StorageDriver.List(ctx, path)
}

func (d *resumeDigestCountingDriver) resetCounts() {
	d.getContentCalls = 0
	d.listCalls = 0
}

func (d *resumeDigestCountingDriver) storageCalls() int {
	return d.getContentCalls + d.listCalls
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) {
	return 0, io.EOF
}
