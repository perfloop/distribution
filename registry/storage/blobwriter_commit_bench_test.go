package storage

import (
	"context"
	"crypto/rand"
	_ "crypto/sha256"
	"testing"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

var benchmarkBlobWriterCommitDescriptor v1.Descriptor

func BenchmarkBlobWriterCommitFallbackSHA256_8MiB(b *testing.B) {
	benchmarkBlobWriterCommitFallbackSHA256(b, 8<<20)
}

func BenchmarkBlobWriterCommitFallbackSHA256_64MiB(b *testing.B) {
	benchmarkBlobWriterCommitFallbackSHA256(b, 64<<20)
}

func BenchmarkBlobWriterCommitFallbackSHA256_256MiB(b *testing.B) {
	benchmarkBlobWriterCommitFallbackSHA256(b, 256<<20)
}

func benchmarkBlobWriterCommitFallbackSHA256(b *testing.B, size int) {
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		b.Fatalf("populate commit payload: %v", err)
	}
	payloadDigest := digest.Canonical.FromBytes(payload)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		blobs := newFilesystemCommitBlobStore(b, ctx)
		upload := prepareFallbackCommit(b, ctx, blobs, payload)
		b.StartTimer()

		desc, err := upload.Commit(ctx, v1.Descriptor{
			Digest: payloadDigest,
			Size:   int64(len(payload)),
		})

		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if desc.Digest != payloadDigest {
			b.Fatalf("Commit() digest = %v, want %v", desc.Digest, payloadDigest)
		}
		benchmarkBlobWriterCommitDescriptor = desc
	}
}

func newFilesystemCommitBlobStore(tb testing.TB, ctx context.Context) distribution.BlobIngester {
	tb.Helper()

	driver := filesystem.New(filesystem.DriverParameters{
		RootDirectory: tb.TempDir(),
		MaxThreads:    25,
	})
	registry, err := NewRegistry(ctx, driver, DisableDigestResumption)
	if err != nil {
		tb.Fatalf("create registry: %v", err)
	}
	name, err := reference.WithName("benchmark/blobwriter")
	if err != nil {
		tb.Fatalf("create repository name: %v", err)
	}
	repository, err := registry.Repository(ctx, name)
	if err != nil {
		tb.Fatalf("create repository: %v", err)
	}

	return repository.Blobs(ctx)
}

func prepareFallbackCommit(tb testing.TB, ctx context.Context, blobs distribution.BlobIngester, payload []byte) distribution.BlobWriter {
	tb.Helper()

	upload, err := blobs.Create(ctx)
	if err != nil {
		tb.Fatalf("create upload: %v", err)
	}
	if _, err := upload.Write(payload[:len(payload)-1]); err != nil {
		tb.Fatalf("write upload prefix: %v", err)
	}
	if err := upload.Close(); err != nil {
		tb.Fatalf("close upload prefix: %v", err)
	}

	upload, err = blobs.Resume(ctx, upload.ID())
	if err != nil {
		tb.Fatalf("resume upload: %v", err)
	}
	if _, err := upload.Write(payload[len(payload)-1:]); err != nil {
		tb.Fatalf("write upload suffix: %v", err)
	}

	writer, ok := upload.(*blobWriter)
	if !ok {
		tb.Fatalf("resumed upload type = %T, want *blobWriter", upload)
	}
	if writer.written == writer.Size() {
		tb.Fatal("resumed upload did not enter full-hash fallback")
	}

	return upload
}
