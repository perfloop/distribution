//go:build !noresumabledigest

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/distribution/distribution/v3"
	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// recordingStorageDriver records the storage operations that make up upload
// finalization and can fail the boundaries that Commit must preserve.
type recordingStorageDriver struct {
	storagedriver.StorageDriver

	calls []storageCall

	moveErr    error
	deleteErr  error
	linkPutErr error
	closeErr   error
}

type storageCall struct {
	operation   string
	path        string
	destination string
}

func (call storageCall) String() string {
	if call.destination != "" {
		return fmt.Sprintf("%s(%s->%s)", call.operation, storagePathRole(call.path), storagePathRole(call.destination))
	}

	return fmt.Sprintf("%s(%s)", call.operation, storagePathRole(call.path))
}

func storagePathRole(storagePath string) string {
	switch {
	case strings.Contains(storagePath, "/_uploads/") && strings.Contains(storagePath, "/hashstates/"):
		return "hash-state"
	case strings.Contains(storagePath, "/_uploads/") && strings.HasSuffix(storagePath, "/startedat"):
		return "started-at"
	case strings.Contains(storagePath, "/_uploads/") && strings.HasSuffix(storagePath, "/data"):
		return "upload"
	case strings.Contains(storagePath, "/_uploads/"):
		return "upload-directory"
	case strings.Contains(storagePath, "/_layers/") && strings.HasSuffix(storagePath, "/link"):
		return "link"
	case strings.Contains(storagePath, "/blobs/") && strings.HasSuffix(storagePath, "/data"):
		return "blob"
	default:
		return storagePath
	}
}

func (driver *recordingStorageDriver) record(operation, storagePath, destination string) {
	driver.calls = append(driver.calls, storageCall{
		operation:   operation,
		path:        storagePath,
		destination: destination,
	})
}

func (driver *recordingStorageDriver) resetCalls() {
	driver.calls = nil
}

func (driver *recordingStorageDriver) recordedCalls() []string {
	calls := make([]string, len(driver.calls))
	for i, call := range driver.calls {
		calls[i] = call.String()
	}

	return calls
}

func (driver *recordingStorageDriver) GetContent(ctx context.Context, storagePath string) ([]byte, error) {
	driver.record("GetContent", storagePath, "")
	return driver.StorageDriver.GetContent(ctx, storagePath)
}

func (driver *recordingStorageDriver) PutContent(ctx context.Context, storagePath string, content []byte) error {
	driver.record("PutContent", storagePath, "")
	if storagePathRole(storagePath) == "link" && driver.linkPutErr != nil {
		return driver.linkPutErr
	}

	return driver.StorageDriver.PutContent(ctx, storagePath, content)
}

func (driver *recordingStorageDriver) Reader(ctx context.Context, storagePath string, offset int64) (io.ReadCloser, error) {
	driver.record("Reader", storagePath, "")
	return driver.StorageDriver.Reader(ctx, storagePath, offset)
}

func (driver *recordingStorageDriver) Writer(ctx context.Context, storagePath string, appendMode bool) (storagedriver.FileWriter, error) {
	driver.record("Writer", storagePath, "")
	writer, err := driver.StorageDriver.Writer(ctx, storagePath, appendMode)
	if err != nil {
		return nil, err
	}

	return &recordingFileWriter{
		FileWriter: writer,
		driver:     driver,
		path:       storagePath,
	}, nil
}

func (driver *recordingStorageDriver) Stat(ctx context.Context, storagePath string) (storagedriver.FileInfo, error) {
	driver.record("Stat", storagePath, "")
	return driver.StorageDriver.Stat(ctx, storagePath)
}

func (driver *recordingStorageDriver) List(ctx context.Context, storagePath string) ([]string, error) {
	driver.record("List", storagePath, "")
	return driver.StorageDriver.List(ctx, storagePath)
}

func (driver *recordingStorageDriver) Move(ctx context.Context, sourcePath, destinationPath string) error {
	driver.record("Move", sourcePath, destinationPath)
	if driver.moveErr != nil {
		return driver.moveErr
	}

	return driver.StorageDriver.Move(ctx, sourcePath, destinationPath)
}

func (driver *recordingStorageDriver) Delete(ctx context.Context, storagePath string) error {
	driver.record("Delete", storagePath, "")
	if driver.deleteErr != nil {
		return driver.deleteErr
	}

	return driver.StorageDriver.Delete(ctx, storagePath)
}

type recordingFileWriter struct {
	storagedriver.FileWriter
	driver *recordingStorageDriver
	path   string
}

func (writer *recordingFileWriter) Close() error {
	writer.driver.record("FileWriter.Close", writer.path, "")
	if err := writer.FileWriter.Close(); err != nil {
		return err
	}

	return writer.driver.closeErr
}

func (writer *recordingFileWriter) Commit(ctx context.Context) error {
	writer.driver.record("FileWriter.Commit", writer.path, "")
	return writer.FileWriter.Commit(ctx)
}

type blobWriterCommitFixture struct {
	ctx           context.Context
	driver        *recordingStorageDriver
	storageDriver storagedriver.StorageDriver
	blobs         distribution.BlobStore
}

func newBlobWriterCommitFixture(tb testing.TB) *blobWriterCommitFixture {
	tb.Helper()

	driver := &recordingStorageDriver{StorageDriver: inmemory.New()}
	fixture := newBlobWriterCommitFixtureForDriver(tb, driver)
	fixture.driver = driver
	return fixture
}

func newBlobWriterCommitFixtureForDriver(tb testing.TB, driver storagedriver.StorageDriver) *blobWriterCommitFixture {
	tb.Helper()

	ctx := context.Background()
	repositoryName, err := reference.WithName("commit/test")
	if err != nil {
		tb.Fatal(err)
	}

	registry, err := NewRegistry(ctx, driver)
	if err != nil {
		tb.Fatal(err)
	}
	repository, err := registry.Repository(ctx, repositoryName)
	if err != nil {
		tb.Fatal(err)
	}

	return &blobWriterCommitFixture{
		ctx:           ctx,
		storageDriver: driver,
		blobs:         repository.Blobs(ctx),
	}
}

func newFilesystemBlobWriterCommitFixture(tb testing.TB) *blobWriterCommitFixture {
	tb.Helper()

	driver, err := filesystem.FromParameters(map[string]any{
		"rootdirectory": tb.TempDir(),
	})
	if err != nil {
		tb.Fatal(err)
	}

	return newBlobWriterCommitFixtureForDriver(tb, driver)
}

func newInMemoryBlobWriterCommitFixture(tb testing.TB) *blobWriterCommitFixture {
	tb.Helper()

	return newBlobWriterCommitFixtureForDriver(tb, inmemory.New())
}

func (fixture *blobWriterCommitFixture) newUpload(tb testing.TB, payload []byte) (distribution.BlobWriter, digest.Digest, string) {
	tb.Helper()

	writer, err := fixture.blobs.Create(fixture.ctx)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		tb.Fatal(err)
	}

	blobWriter, ok := writer.(*blobWriter)
	if !ok {
		tb.Fatalf("writer type = %T, want *blobWriter", writer)
	}

	return writer, digest.FromBytes(payload), blobWriter.path
}

func assertDescriptor(t *testing.T, descriptor v1.Descriptor, wantDigest digest.Digest, wantSize int) {
	t.Helper()

	if descriptor.Digest != wantDigest || descriptor.Size != int64(wantSize) {
		t.Fatalf("descriptor = %#v, want digest %s and size %d", descriptor, wantDigest, wantSize)
	}
}

func assertCommittedBlob(t *testing.T, fixture *blobWriterCommitFixture, dgst digest.Digest, want []byte) {
	t.Helper()

	blobPath, err := pathFor(blobDataPathSpec{digest: dgst})
	if err != nil {
		t.Fatal(err)
	}

	got, err := fixture.storageDriver.GetContent(fixture.ctx, blobPath)
	if err != nil {
		t.Fatalf("GetContent(%q) = %v", blobPath, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("blob bytes = %q, want %q", got, want)
	}
}

func assertUploadRemoved(t *testing.T, fixture *blobWriterCommitFixture, uploadPath string) {
	t.Helper()

	_, err := fixture.storageDriver.List(fixture.ctx, path.Dir(uploadPath))
	if err == nil {
		t.Fatalf("upload directory %q still exists", path.Dir(uploadPath))
	}

	var notFound storagedriver.PathNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("List(%q) error = %T %v, want PathNotFoundError", path.Dir(uploadPath), err, err)
	}
}

func assertHashStatePresent(t *testing.T, fixture *blobWriterCommitFixture, writer distribution.BlobWriter) {
	t.Helper()

	blobWriter, ok := writer.(*blobWriter)
	if !ok {
		t.Fatalf("writer type = %T, want *blobWriter", writer)
	}

	hashStatePath, err := pathFor(uploadHashStatePathSpec{
		name:   blobWriter.blobStore.repository.Named().String(),
		id:     blobWriter.id,
		alg:    blobWriter.digester.Digest().Algorithm(),
		offset: blobWriter.written,
	})
	if err != nil {
		t.Fatal(err)
	}

	state, err := fixture.storageDriver.GetContent(fixture.ctx, hashStatePath)
	if err != nil {
		t.Fatalf("GetContent(%q) = %v", hashStatePath, err)
	}
	if len(state) == 0 {
		t.Fatalf("hash state %q is empty", hashStatePath)
	}
}

func assertCalls(t *testing.T, driver *recordingStorageDriver, want ...string) {
	t.Helper()

	got := driver.recordedCalls()
	if len(got) != len(want) {
		t.Fatalf("storage calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("storage call %d = %q, want %q; all calls = %v", i, got[i], want[i], got)
		}
	}
}

func expectedFinalCommitCalls(includeHashState bool) []string {
	calls := []string{"FileWriter.Commit(upload)"}
	if includeHashState {
		calls = append(calls, "PutContent(hash-state)")
	}

	return append(calls,
		"FileWriter.Close(upload)",
		"Stat(upload)",
		"Stat(blob)",
		"Stat(upload)",
		"Move(upload->blob)",
		"PutContent(link)",
		"Delete(upload-directory)",
	)
}

func expectedValidationFailureCalls(includeHashState bool) []string {
	calls := []string{"FileWriter.Commit(upload)"}
	if includeHashState {
		calls = append(calls, "PutContent(hash-state)")
	}

	return append(calls, "FileWriter.Close(upload)", "Stat(upload)")
}

func expectedMoveFailureCalls(includeHashState bool) []string {
	calls := expectedValidationFailureCalls(includeHashState)
	return append(calls, "Stat(blob)", "Stat(upload)", "Move(upload->blob)")
}

func expectedLinkFailureCalls(includeHashState bool) []string {
	calls := expectedMoveFailureCalls(includeHashState)
	return append(calls, "PutContent(link)")
}

func TestBlobWriterCloseStoresResumableStateOnce(t *testing.T) {
	fixture := newBlobWriterCommitFixture(t)
	writer, _, _ := fixture.newUpload(t, []byte("chunked upload state"))

	fixture.driver.resetCalls()
	if err := writer.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}

	assertCalls(t, fixture.driver,
		"PutContent(hash-state)",
		"FileWriter.Close(upload)",
	)
	assertHashStatePresent(t, fixture, writer)
}

func TestBlobWriterCommitStorageSequence(t *testing.T) {
	payload := []byte("final upload storage sequence")
	fixture := newBlobWriterCommitFixture(t)
	writer, wantDigest, uploadPath := fixture.newUpload(t, payload)

	fixture.driver.resetCalls()
	descriptor, err := writer.Commit(fixture.ctx, v1.Descriptor{Digest: wantDigest})
	if err != nil {
		t.Fatalf("Commit = %v", err)
	}

	assertDescriptor(t, descriptor, wantDigest, len(payload))
	assertCalls(t, fixture.driver, expectedFinalCommitCalls(true)...)
	assertCommittedBlob(t, fixture, wantDigest, payload)
	assertUploadRemoved(t, fixture, uploadPath)
}

func TestBlobWriterCommitFailureBoundaries(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		payload := []byte("validation retry payload")
		fixture := newBlobWriterCommitFixture(t)
		writer, wantDigest, uploadPath := fixture.newUpload(t, payload)

		fixture.driver.resetCalls()
		_, err := writer.Commit(fixture.ctx, v1.Descriptor{Digest: digest.FromBytes([]byte("incorrect digest"))})
		var invalidDigest distribution.ErrBlobInvalidDigest
		if !errors.As(err, &invalidDigest) {
			t.Fatalf("Commit error = %T %v, want ErrBlobInvalidDigest", err, err)
		}
		assertCalls(t, fixture.driver, expectedValidationFailureCalls(true)...)

		resumed, err := fixture.blobs.Resume(fixture.ctx, writer.ID())
		if err != nil {
			t.Fatalf("Resume = %v", err)
		}
		descriptor, err := resumed.Commit(fixture.ctx, v1.Descriptor{Digest: wantDigest})
		if err != nil {
			t.Fatalf("resumed Commit = %v", err)
		}
		assertDescriptor(t, descriptor, wantDigest, len(payload))
		assertCommittedBlob(t, fixture, wantDigest, payload)
		assertUploadRemoved(t, fixture, uploadPath)
	})

	t.Run("close", func(t *testing.T) {
		closeErr := errors.New("close failure")
		payload := []byte("close failure payload")
		fixture := newBlobWriterCommitFixture(t)
		fixture.driver.closeErr = closeErr
		writer, wantDigest, uploadPath := fixture.newUpload(t, payload)

		fixture.driver.resetCalls()
		descriptor, err := writer.Commit(fixture.ctx, v1.Descriptor{Digest: wantDigest})
		if err != nil {
			t.Fatalf("Commit = %v, want success despite ignored Close error", err)
		}
		assertDescriptor(t, descriptor, wantDigest, len(payload))
		assertCalls(t, fixture.driver, expectedFinalCommitCalls(true)...)
		assertCommittedBlob(t, fixture, wantDigest, payload)
		assertUploadRemoved(t, fixture, uploadPath)
	})

	t.Run("move", func(t *testing.T) {
		moveErr := errors.New("move failure")
		payload := []byte("move retry payload")
		fixture := newBlobWriterCommitFixture(t)
		fixture.driver.moveErr = moveErr
		writer, wantDigest, uploadPath := fixture.newUpload(t, payload)

		fixture.driver.resetCalls()
		_, err := writer.Commit(fixture.ctx, v1.Descriptor{Digest: wantDigest})
		if err != moveErr {
			t.Fatalf("Commit error = %v, want original move error %v", err, moveErr)
		}
		assertCalls(t, fixture.driver, expectedMoveFailureCalls(true)...)

		fixture.driver.moveErr = nil
		resumed, err := fixture.blobs.Resume(fixture.ctx, writer.ID())
		if err != nil {
			t.Fatalf("Resume = %v", err)
		}
		descriptor, err := resumed.Commit(fixture.ctx, v1.Descriptor{Digest: wantDigest})
		if err != nil {
			t.Fatalf("resumed Commit = %v", err)
		}
		assertDescriptor(t, descriptor, wantDigest, len(payload))
		assertCommittedBlob(t, fixture, wantDigest, payload)
		assertUploadRemoved(t, fixture, uploadPath)
	})

	t.Run("link", func(t *testing.T) {
		linkErr := errors.New("link failure")
		payload := []byte("link failure payload")
		fixture := newBlobWriterCommitFixture(t)
		fixture.driver.linkPutErr = linkErr
		writer, wantDigest, _ := fixture.newUpload(t, payload)

		fixture.driver.resetCalls()
		_, err := writer.Commit(fixture.ctx, v1.Descriptor{Digest: wantDigest})
		if err != linkErr {
			t.Fatalf("Commit error = %v, want original link error %v", err, linkErr)
		}
		assertCalls(t, fixture.driver, expectedLinkFailureCalls(true)...)
		assertCommittedBlob(t, fixture, wantDigest, payload)
	})

	t.Run("cleanup", func(t *testing.T) {
		cleanupErr := errors.New("cleanup failure")
		payload := []byte("cleanup failure payload")
		fixture := newBlobWriterCommitFixture(t)
		fixture.driver.deleteErr = cleanupErr
		writer, wantDigest, uploadPath := fixture.newUpload(t, payload)

		fixture.driver.resetCalls()
		_, err := writer.Commit(fixture.ctx, v1.Descriptor{Digest: wantDigest})
		if err != cleanupErr {
			t.Fatalf("Commit error = %v, want original cleanup error %v", err, cleanupErr)
		}
		assertCalls(t, fixture.driver, expectedFinalCommitCalls(true)...)
		assertCommittedBlob(t, fixture, wantDigest, payload)

		fixture.driver.deleteErr = nil
		blobWriter := writer.(*blobWriter)
		if err := blobWriter.removeResources(fixture.ctx); err != nil {
			t.Fatalf("removeResources after cleanup failure = %v", err)
		}
		assertUploadRemoved(t, fixture, uploadPath)
	})
}

func BenchmarkBlobWriterCommitFilesystem(b *testing.B) {
	benchmarkBlobWriterCommit(b, newFilesystemBlobWriterCommitFixture(b))
}

func BenchmarkBlobWriterCommitInMemoryZeroLatency(b *testing.B) {
	benchmarkBlobWriterCommit(b, newInMemoryBlobWriterCommitFixture(b))
}

func benchmarkBlobWriterCommit(b *testing.B, fixture *blobWriterCommitFixture) {
	payload := make([]byte, 32*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		binary.LittleEndian.PutUint64(payload[len(payload)-8:], uint64(i))
		writer, wantDigest, _ := fixture.newUpload(b, payload)
		b.StartTimer()

		descriptor, err := writer.Commit(fixture.ctx, v1.Descriptor{Digest: wantDigest})

		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if descriptor.Digest != wantDigest || descriptor.Size != int64(len(payload)) {
			b.Fatalf("descriptor = %#v, want digest %s and size %d", descriptor, wantDigest, len(payload))
		}
	}
}
