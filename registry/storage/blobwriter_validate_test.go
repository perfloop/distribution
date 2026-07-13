package storage

import (
	"context"
	"crypto/rand"
	_ "crypto/sha256"
	_ "crypto/sha512"
	"testing"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const benchmarkBlobWriterValidationSize = 128 << 20

var benchmarkBlobWriterValidationDescriptor v1.Descriptor

func TestBlobWriterValidateBlobFallback(t *testing.T) {
	payload := []byte("fallback validation must verify the entire stored blob")
	canonical := digest.Canonical.FromBytes(payload)

	tests := []struct {
		name       string
		digest     digest.Digest
		wantErr    bool
		wantDigest digest.Digest
	}{
		{
			name:       "canonical digest",
			digest:     canonical,
			wantDigest: canonical,
		},
		{
			name:       "alternate digest",
			digest:     digest.SHA512.FromBytes(payload),
			wantDigest: canonical,
		},
		{
			name:    "mismatched canonical digest",
			digest:  digest.Canonical.FromBytes([]byte("different content")),
			wantErr: true,
		},
		{
			name:    "mismatched alternate digest",
			digest:  digest.SHA512.FromBytes([]byte("different content")),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bw := newFallbackValidationBlobWriter(t, payload)
			desc, err := bw.validateBlob(context.Background(), v1.Descriptor{
				Digest: tc.digest,
				Size:   int64(len(payload)),
			})
			if tc.wantErr {
				if _, ok := err.(distribution.ErrBlobInvalidDigest); !ok {
					t.Fatalf("validateBlob error = %v, want ErrBlobInvalidDigest", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateBlob() error = %v", err)
			}
			if desc.Digest != tc.wantDigest {
				t.Fatalf("validateBlob() digest = %v, want %v", desc.Digest, tc.wantDigest)
			}
			if desc.Size != int64(len(payload)) {
				t.Fatalf("validateBlob() size = %d, want %d", desc.Size, len(payload))
			}
			if desc.MediaType != "application/octet-stream" {
				t.Fatalf("validateBlob() media type = %q, want application/octet-stream", desc.MediaType)
			}
		})
	}
}

func TestBlobWriterValidateBlobResumedCanonicalDigest(t *testing.T) {
	payload := []byte("a resumable digest must remain the canonical descriptor")
	canonical := digest.Canonical.FromBytes(payload)
	bw := newResumedValidationBlobWriter(t, payload)

	desc, err := bw.validateBlob(context.Background(), v1.Descriptor{
		Digest: digest.SHA512.FromBytes(payload),
		Size:   int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("validateBlob() error = %v", err)
	}
	if desc.Digest != canonical {
		t.Fatalf("validateBlob() digest = %v, want %v", desc.Digest, canonical)
	}

	_, err = bw.validateBlob(context.Background(), v1.Descriptor{
		Digest: digest.SHA512.FromBytes([]byte("different content")),
		Size:   int64(len(payload)),
	})
	if _, ok := err.(distribution.ErrBlobInvalidDigest); !ok {
		t.Fatalf("validateBlob mismatched SHA-512 error = %v, want ErrBlobInvalidDigest", err)
	}

	t.Run("canonical digest follows reader content", func(t *testing.T) {
		storedPayload := append([]byte(nil), payload...)
		storedPayload[0] ^= 0xff

		ctx := context.Background()
		driver := inmemory.New()
		const blobPath = "/validation/changed-blob"
		fileWriter, err := driver.Writer(ctx, blobPath, false)
		if err != nil {
			t.Fatalf("open validation file writer: %v", err)
		}
		if _, err := fileWriter.Write(storedPayload); err != nil {
			t.Fatalf("write stored payload: %v", err)
		}
		if err := fileWriter.Close(); err != nil {
			t.Fatalf("close validation file writer: %v", err)
		}

		digester := digest.Canonical.Digester()
		if _, err := digester.Hash().Write(payload); err != nil {
			t.Fatalf("seed canonical digester: %v", err)
		}
		resumed := &blobWriter{
			ctx:                    ctx,
			driver:                 driver,
			path:                   blobPath,
			digester:               digester,
			written:                int64(len(payload)),
			fileWriter:             fileWriter,
			resumableDigestEnabled: true,
		}

		desc, err := resumed.validateBlob(ctx, v1.Descriptor{
			Digest: digest.SHA512.FromBytes(storedPayload),
			Size:   int64(len(storedPayload)),
		})
		if err != nil {
			t.Fatalf("validateBlob() error = %v", err)
		}
		want := digest.Canonical.FromBytes(storedPayload)
		if desc.Digest != want {
			t.Fatalf("validateBlob() digest = %v, want reader digest %v", desc.Digest, want)
		}
	})
}

func BenchmarkBlobWriterValidateBlobFallbackSHA256(b *testing.B) {
	payload := benchmarkValidationPayload(b)
	canonical := digest.Canonical.FromBytes(payload)
	bw := newFallbackValidationBlobWriter(b, payload)
	desc := v1.Descriptor{Digest: canonical, Size: int64(len(payload))}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validated, err := bw.validateBlob(context.Background(), desc)
		if err != nil {
			b.Fatal(err)
		}
		if validated.Digest != canonical {
			b.Fatalf("validateBlob() digest = %v, want %v", validated.Digest, canonical)
		}
		benchmarkBlobWriterValidationDescriptor = validated
	}
}

func BenchmarkBlobWriterValidateBlobFallbackSHA512(b *testing.B) {
	payload := benchmarkValidationPayload(b)
	canonical := digest.Canonical.FromBytes(payload)
	bw := newFallbackValidationBlobWriter(b, payload)
	desc := v1.Descriptor{Digest: digest.SHA512.FromBytes(payload), Size: int64(len(payload))}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validated, err := bw.validateBlob(context.Background(), desc)
		if err != nil {
			b.Fatal(err)
		}
		if validated.Digest != canonical {
			b.Fatalf("validateBlob() digest = %v, want %v", validated.Digest, canonical)
		}
		benchmarkBlobWriterValidationDescriptor = validated
	}
}

func BenchmarkBlobWriterValidateBlobResumedSHA512(b *testing.B) {
	payload := benchmarkValidationPayload(b)
	canonical := digest.Canonical.FromBytes(payload)
	bw := newResumedValidationBlobWriter(b, payload)
	desc := v1.Descriptor{Digest: digest.SHA512.FromBytes(payload), Size: int64(len(payload))}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		validated, err := bw.validateBlob(context.Background(), desc)
		if err != nil {
			b.Fatal(err)
		}
		if validated.Digest != canonical {
			b.Fatalf("validateBlob() digest = %v, want %v", validated.Digest, canonical)
		}
		benchmarkBlobWriterValidationDescriptor = validated
	}
}

func benchmarkValidationPayload(tb testing.TB) []byte {
	tb.Helper()

	payload := make([]byte, benchmarkBlobWriterValidationSize)
	if _, err := rand.Read(payload); err != nil {
		tb.Fatalf("populate validation payload: %v", err)
	}

	return payload
}

func newFallbackValidationBlobWriter(tb testing.TB, payload []byte) *blobWriter {
	tb.Helper()

	ctx := context.Background()
	driver := inmemory.New()
	const blobPath = "/validation/blob"
	if err := driver.PutContent(ctx, blobPath, payload); err != nil {
		tb.Fatalf("store validation payload: %v", err)
	}

	return &blobWriter{
		ctx:                    ctx,
		driver:                 driver,
		path:                   blobPath,
		digester:               digest.Canonical.Digester(),
		resumableDigestEnabled: false,
	}
}

func newResumedValidationBlobWriter(tb testing.TB, payload []byte) *blobWriter {
	tb.Helper()

	ctx := context.Background()
	driver := inmemory.New()
	const blobPath = "/validation/blob"
	if err := driver.PutContent(ctx, blobPath, payload); err != nil {
		tb.Fatalf("store validation payload: %v", err)
	}

	fileWriter, err := driver.Writer(ctx, blobPath, true)
	if err != nil {
		tb.Fatalf("open validation file writer: %v", err)
	}
	if err := fileWriter.Close(); err != nil {
		tb.Fatalf("close validation file writer: %v", err)
	}

	digester := digest.Canonical.Digester()
	if _, err := digester.Hash().Write(payload); err != nil {
		tb.Fatalf("seed canonical digester: %v", err)
	}

	return &blobWriter{
		ctx:                    ctx,
		driver:                 driver,
		path:                   blobPath,
		digester:               digester,
		written:                int64(len(payload)),
		fileWriter:             fileWriter,
		resumableDigestEnabled: true,
	}
}
