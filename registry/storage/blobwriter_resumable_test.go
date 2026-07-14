//go:build !noresumabledigest

package storage

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/distribution/distribution/v3"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
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
		name                 string
		removeCheckpoint     bool
		legacyCheckpointName string
		restoresDigest       bool
	}{
		{name: "exact-checkpoint", restoresDigest: true},
		{name: "missing-checkpoint", removeCheckpoint: true},
		{name: "legacy-hex-checkpoint", legacyCheckpointName: "0x11", restoresDigest: true},
		{name: "legacy-octal-checkpoint", legacyCheckpointName: "021", restoresDigest: true},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			for _, checkpointState := range checkpointStates {
				t.Run(checkpointState.name, func(t *testing.T) {
					driver := &resumeDigestTracingDriver{StorageDriver: inmemory.New()}
					ctx, blobs, upload, initial := newResumableDigestUpload(t, driver)
					if checkpointState.removeCheckpoint {
						removeResumableDigestCheckpoint(t, ctx, driver, upload)
					}
					if checkpointState.legacyCheckpointName != "" {
						replaceResumableDigestCheckpoint(t, ctx, driver, upload, checkpointState.legacyCheckpointName)
					}

					resumed, err := blobs.Resume(ctx, upload.ID())
					if err != nil {
						t.Fatalf("resume upload: %v", err)
					}
					driver.resetTrace()

					suffix := []byte("next chunk")
					n, err := operation.write(resumed, suffix)
					if err != nil {
						t.Fatalf("append upload: %v", err)
					}
					if n != int64(len(suffix)) {
						t.Fatalf("appended %d bytes, want %d", n, len(suffix))
					}

					resumedWriter, ok := resumed.(*blobWriter)
					if !ok {
						t.Fatalf("resumed writer type = %T, want *blobWriter", resumed)
					}
					wantWritten := int64(len(suffix))
					if checkpointState.restoresDigest {
						wantWritten += int64(len(initial))
					}
					if resumedWriter.written != wantWritten {
						t.Fatalf("resumed writer digest offset = %d, want %d", resumedWriter.written, wantWritten)
					}
					t.Logf("storage waterfall: %s", driver.waterfall())

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

	testBlobWriterResumeDigestFilesystemWaterfalls(t)
}

func testBlobWriterResumeDigestFilesystemWaterfalls(t *testing.T) {
	t.Helper()

	payload := bytes.Repeat([]byte("x"), resumeDigestLifecyclePayloadSize)
	for _, testCase := range []struct {
		name             string
		checkpointCount  int
		removeCheckpoint bool
	}{
		{name: "exact-1", checkpointCount: 1},
		{name: "exact-128", checkpointCount: resumeDigestLifecycleCheckpointCount},
		{name: "missing-1", checkpointCount: 1, removeCheckpoint: true},
		{name: "missing-128", checkpointCount: resumeDigestLifecycleCheckpointCount, removeCheckpoint: true},
	} {
		t.Run("filesystem-lifecycle-"+testCase.name, func(t *testing.T) {
			driver := &resumeDigestTracingDriver{StorageDriver: filesystem.New(filesystem.DriverParameters{
				RootDirectory: t.TempDir(),
				MaxThreads:    25,
			})}
			ctx, blobs, upload, initial := newResumableDigestUpload(t, driver)
			addResumableDigestCheckpointFiles(t, ctx, driver, upload, testCase.checkpointCount)
			if testCase.removeCheckpoint {
				removeResumableDigestCheckpoint(t, ctx, driver, upload)
			}

			driver.resetTrace()
			started := time.Now()
			resumed, err := blobs.Resume(ctx, upload.ID())
			if err != nil {
				t.Fatalf("resume upload: %v", err)
			}
			n, err := resumed.ReadFrom(bytes.NewReader(payload))
			if err != nil {
				t.Fatalf("append resumed upload: %v", err)
			}
			if n != int64(len(payload)) {
				t.Fatalf("appended %d bytes, want %d", n, len(payload))
			}
			resumedWriter, ok := resumed.(*blobWriter)
			if !ok {
				t.Fatalf("resumed writer type = %T, want *blobWriter", resumed)
			}
			wantWritten := int64(len(payload))
			if !testCase.removeCheckpoint {
				wantWritten += int64(len(initial))
			}
			if resumedWriter.written != wantWritten {
				t.Fatalf("resumed writer digest offset = %d, want %d", resumedWriter.written, wantWritten)
			}
			if err := resumed.Close(); err != nil {
				t.Fatalf("close resumed upload: %v", err)
			}
			t.Logf("storage waterfall total=%s: %s", time.Since(started), driver.waterfall())
		})
	}
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
	hashStatePath := resumableDigestCheckpointPathForTest(tb, writer, writer.written)
	if err := driver.Delete(ctx, hashStatePath); err != nil {
		tb.Fatalf("delete hash state: %v", err)
	}
}

func replaceResumableDigestCheckpoint(tb testing.TB, ctx context.Context, driver storagedriver.StorageDriver, upload distribution.BlobWriter, legacyName string) {
	tb.Helper()

	writer, ok := upload.(*blobWriter)
	if !ok {
		tb.Fatalf("upload writer type = %T, want *blobWriter", upload)
	}
	canonicalPath := resumableDigestCheckpointPathForTest(tb, writer, writer.written)
	state, err := driver.GetContent(ctx, canonicalPath)
	if err != nil {
		tb.Fatalf("read canonical hash state: %v", err)
	}
	if err := driver.Delete(ctx, canonicalPath); err != nil {
		tb.Fatalf("delete canonical hash state: %v", err)
	}
	legacyPath := path.Join(path.Dir(canonicalPath), legacyName)
	if err := driver.PutContent(ctx, legacyPath, state); err != nil {
		tb.Fatalf("write legacy hash state: %v", err)
	}

	listPath, err := pathFor(uploadHashStatePathSpec{
		name: writer.blobStore.repository.Named().String(),
		id:   writer.id,
		alg:  writer.digester.Digest().Algorithm(),
		list: true,
	})
	if err != nil {
		tb.Fatalf("create hash state list path: %v", err)
	}
	paths, err := driver.List(ctx, listPath)
	if err != nil {
		tb.Fatalf("list raw hash states: %v", err)
	}
	if containsPath(paths, canonicalPath) {
		tb.Fatalf("raw listing unexpectedly contains canonical checkpoint %q", canonicalPath)
	}
	if !containsPath(paths, legacyPath) {
		tb.Fatalf("raw listing does not contain legacy checkpoint %q: %v", legacyPath, paths)
	}
}

func resumableDigestCheckpointPathForTest(tb testing.TB, writer *blobWriter, offset int64) string {
	tb.Helper()

	hashStatePath, err := pathFor(uploadHashStatePathSpec{
		name:   writer.blobStore.repository.Named().String(),
		id:     writer.id,
		alg:    writer.digester.Digest().Algorithm(),
		offset: offset,
	})
	if err != nil {
		tb.Fatalf("create hash state path: %v", err)
	}
	return hashStatePath
}

func containsPath(paths []string, want string) bool {
	for _, storedPath := range paths {
		if storedPath == want {
			return true
		}
	}
	return false
}

type resumeDigestTraceEvent struct {
	operation string
	path      string
	duration  time.Duration
}

type resumeDigestTracingDriver struct {
	storagedriver.StorageDriver
	events []resumeDigestTraceEvent
}

func (d *resumeDigestTracingDriver) GetContent(ctx context.Context, storedPath string) ([]byte, error) {
	started := time.Now()
	content, err := d.StorageDriver.GetContent(ctx, storedPath)
	d.record("GetContent", storedPath, started)
	return content, err
}

func (d *resumeDigestTracingDriver) List(ctx context.Context, storedPath string) ([]string, error) {
	started := time.Now()
	paths, err := d.StorageDriver.List(ctx, storedPath)
	d.record("List", storedPath, started)
	return paths, err
}

func (d *resumeDigestTracingDriver) PutContent(ctx context.Context, storedPath string, content []byte) error {
	started := time.Now()
	err := d.StorageDriver.PutContent(ctx, storedPath, content)
	d.record("PutContent", storedPath, started)
	return err
}

func (d *resumeDigestTracingDriver) Writer(ctx context.Context, storedPath string, appendMode bool) (storagedriver.FileWriter, error) {
	started := time.Now()
	writer, err := d.StorageDriver.Writer(ctx, storedPath, appendMode)
	d.record("Writer", storedPath, started)
	return writer, err
}

func (d *resumeDigestTracingDriver) resetTrace() {
	d.events = nil
}

func (d *resumeDigestTracingDriver) record(operation, storedPath string, started time.Time) {
	d.events = append(d.events, resumeDigestTraceEvent{
		operation: operation,
		path:      storedPath,
		duration:  time.Since(started),
	})
}

func (d *resumeDigestTracingDriver) waterfall() string {
	if len(d.events) == 0 {
		return "no storage calls"
	}

	events := make([]string, 0, len(d.events))
	for _, event := range d.events {
		events = append(events, fmt.Sprintf("%s[%s]=%s", event.operation, path.Base(event.path), event.duration))
	}
	return strings.Join(events, " -> ")
}
