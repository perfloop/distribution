//go:build !noresumabledigest

package storage

import (
	"context"
	"encoding"
	"fmt"
	"hash"
	"path"
	"strconv"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/sirupsen/logrus"
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

		// State written by current versions always has a decimal name. Look up
		// a legacy numeric name only after that direct lookup misses, preserving
		// compatibility with state the former List-based lookup could restore.
		storedState, found, err := bw.getStoredHashState(ctx, offset)
		if err != nil {
			return err
		}
		if !found {
			// No matching state is available, so reset the hasher.
			h.(hash.Hash).Reset()
		} else {
			if err = h.UnmarshalBinary(storedState); err != nil {
				return err
			}
			bw.written = offset
		}
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

// getStoredHashState looks for a legacy state path whose base-0 numeric suffix
// equals offset. Current writes use a decimal path and are loaded directly by
// resumeDigest; this scan is only a compatibility fallback after a direct miss.
func (bw *blobWriter) getStoredHashState(ctx context.Context, offset int64) ([]byte, bool, error) {
	uploadHashStatePathPrefix, err := pathFor(uploadHashStatePathSpec{
		name: bw.blobStore.repository.Named().String(),
		id:   bw.id,
		alg:  bw.digester.Digest().Algorithm(),
		list: true,
	})
	if err != nil {
		return nil, false, err
	}

	paths, err := bw.blobStore.driver.List(ctx, uploadHashStatePathPrefix)
	if err != nil {
		if _, ok := err.(storagedriver.PathNotFoundError); !ok {
			return nil, false, fmt.Errorf("unable to get stored hash states with offset %d: %w", offset, err)
		}
		return nil, false, nil
	}

	for _, storedPath := range paths {
		storedOffset, err := strconv.ParseInt(path.Base(storedPath), 0, 64)
		if err != nil {
			logrus.Errorf("unable to parse offset from upload state path %q: %s", storedPath, err)
			continue
		}
		if storedOffset != offset {
			continue
		}

		storedState, err := bw.driver.GetContent(ctx, storedPath)
		if err != nil {
			return nil, false, err
		}
		return storedState, true, nil
	}

	return nil, false, nil
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
