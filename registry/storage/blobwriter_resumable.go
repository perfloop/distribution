//go:build !noresumabledigest

package storage

import (
	"context"
	"encoding"
	"hash"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
)

// resumeDigest attempts to restore the state of the internal hash function
// by loading the most recent saved hash state equal to the current size of the blob.
func (bw *blobWriter) resumeDigest(ctx context.Context) error {
	if !bw.resumableDigestEnabled {
		return errResumableDigestNotAvailable
	}

	h, ok := bw.digester.Hash().(encoding.BinaryUnmarshaler)
	if !ok {
		return errResumableDigestNotAvailable
	}

	offset := bw.fileWriter.Size()
	if offset == bw.written {
		// State of digester is already at the requested offset.
		return nil
	}

	hashStatePath, err := pathFor(uploadHashStatePathSpec{
		name:   bw.blobStore.repository.Named().String(),
		id:     bw.id,
		alg:    bw.digester.Digest().Algorithm(),
		offset: offset,
	})
	if err != nil {
		return err
	}

	storedState, err := bw.driver.GetContent(ctx, hashStatePath)
	if err != nil {
		if _, ok := err.(storagedriver.PathNotFoundError); !ok {
			return err
		}
		// No matching state is available, so reset the hasher.
		h.(hash.Hash).Reset()
	} else {
		if err = h.UnmarshalBinary(storedState); err != nil {
			return err
		}
		bw.written = offset
	}

	// Mind the gap.
	if gapLen := offset - bw.written; gapLen > 0 {
		return errResumableDigestNotAvailable
	}

	return nil
}

func (bw *blobWriter) storeHashState(ctx context.Context) error {
	if !bw.resumableDigestEnabled {
		return errResumableDigestNotAvailable
	}

	h, ok := bw.digester.Hash().(encoding.BinaryMarshaler)
	if !ok {
		return errResumableDigestNotAvailable
	}

	state, err := h.MarshalBinary()
	if err != nil {
		return err
	}

	uploadHashStatePath, err := pathFor(uploadHashStatePathSpec{
		name:   bw.blobStore.repository.Named().String(),
		id:     bw.id,
		alg:    bw.digester.Digest().Algorithm(),
		offset: bw.written,
	})
	if err != nil {
		return err
	}

	return bw.driver.PutContent(ctx, uploadHashStatePath, state)
}
