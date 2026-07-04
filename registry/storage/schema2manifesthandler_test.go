package storage

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/internal/dcontext"
	"github.com/distribution/distribution/v3/manifest/schema2"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestVerifyManifestForeignLayer(t *testing.T) {
	ctx := dcontext.Background()
	inmemoryDriver := inmemory.New()
	registry := createRegistry(t, inmemoryDriver,
		ManifestURLsAllowRegexp(regexp.MustCompile("^https?://foo")),
		ManifestURLsDenyRegexp(regexp.MustCompile("^https?://foo/nope")),
		EnableValidateImageIndexImagesExist,
	)
	repo := makeRepository(t, registry, "test")
	manifestService := makeManifestService(t, repo)

	config, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeImageConfig, nil)
	if err != nil {
		t.Fatal(err)
	}

	layer, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeLayer, nil)
	if err != nil {
		t.Fatal(err)
	}

	foreignLayer := v1.Descriptor{
		Digest:    "sha256:463435349086340864309863409683460843608348608934092322395278926a",
		Size:      6323,
		MediaType: schema2.MediaTypeForeignLayer,
	}

	emptyLayer := v1.Descriptor{
		Digest: "",
	}

	template := schema2.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: schema2.MediaTypeManifest,
		Config:    config,
	}

	type testcase struct {
		BaseLayer v1.Descriptor
		URLs      []string
		Err       error
	}

	cases := []testcase{
		{
			foreignLayer,
			nil,
			errMissingURL,
		},
		{
			// regular layers may have foreign urls
			layer,
			[]string{"http://foo/bar"},
			nil,
		},
		{
			foreignLayer,
			[]string{"file:///local/file"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"http://foo/bar#baz"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{""},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"https://foo/bar", ""},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"", "https://foo/bar"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"http://nope/bar"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"http://foo/nope"},
			errInvalidURL,
		},
		{
			foreignLayer,
			[]string{"http://foo/bar"},
			nil,
		},
		{
			foreignLayer,
			[]string{"https://foo/bar"},
			nil,
		},
		{
			emptyLayer,
			[]string{"https://foo/empty"},
			digest.ErrDigestInvalidFormat,
		},
		{
			emptyLayer,
			[]string{},
			digest.ErrDigestInvalidFormat,
		},
	}

	for _, c := range cases {
		m := template
		l := c.BaseLayer
		l.URLs = c.URLs
		m.Layers = []v1.Descriptor{l}
		dm, err := schema2.FromStruct(m)
		if err != nil {
			t.Error(err)
			continue
		}

		_, err = manifestService.Put(ctx, dm)
		if verr, ok := err.(distribution.ErrManifestVerification); ok {
			// Extract the first error
			if len(verr) == 2 {
				if _, ok = verr[1].(distribution.ErrManifestBlobUnknown); ok {
					err = verr[0]
				}
			}
		}
		if err != c.Err {
			t.Errorf("%#v: expected %v, got %v", l, c.Err, err)
		}
	}
}

func TestVerifyManifestBlobLayerAndConfig(t *testing.T) {
	ctx := dcontext.Background()
	inmemoryDriver := inmemory.New()
	registry := createRegistry(t, inmemoryDriver,
		ManifestURLsAllowRegexp(regexp.MustCompile("^https?://foo")),
		ManifestURLsDenyRegexp(regexp.MustCompile("^https?://foo/nope")),
		EnableValidateImageIndexImagesExist,
	)

	repo := makeRepository(t, registry, strings.ToLower(t.Name()))
	manifestService := makeManifestService(t, repo)

	config, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeImageConfig, nil)
	if err != nil {
		t.Fatal(err)
	}

	layer, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeLayer, nil)
	if err != nil {
		t.Fatal(err)
	}

	template := schema2.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: schema2.MediaTypeManifest,
	}

	checkFn := func(m schema2.Manifest, rerr error) {
		dm, err := schema2.FromStruct(m)
		if err != nil {
			t.Error(err)
			return
		}

		_, err = manifestService.Put(ctx, dm)
		if verr, ok := err.(distribution.ErrManifestVerification); ok {
			// Extract the first error
			if len(verr) == 2 {
				if _, ok = verr[1].(distribution.ErrManifestBlobUnknown); ok {
					err = verr[0]
				}
			} else if len(verr) == 1 {
				err = verr[0]
			}
		}
		if err != rerr {
			t.Errorf("%#v: expected %v, got %v", m, rerr, err)
		}
	}

	type testcase struct {
		Desc v1.Descriptor
		URLs []string
		Err  error
	}

	layercases := []testcase{
		// empty media type
		{
			v1.Descriptor{},
			[]string{"http://foo/bar"},
			digest.ErrDigestInvalidFormat,
		},
		{
			v1.Descriptor{},
			nil,
			digest.ErrDigestInvalidFormat,
		},
		// unknown media type, but blob is present
		{
			v1.Descriptor{
				Digest: layer.Digest,
			},
			nil,
			nil,
		},
		{
			v1.Descriptor{
				Digest: layer.Digest,
			},
			[]string{"http://foo/bar"},
			nil,
		},
		// gzip layer, but invalid digest
		{
			v1.Descriptor{
				MediaType: schema2.MediaTypeLayer,
			},
			nil,
			digest.ErrDigestInvalidFormat,
		},
		{
			v1.Descriptor{
				MediaType: schema2.MediaTypeLayer,
			},
			[]string{"https://foo/bar"},
			digest.ErrDigestInvalidFormat,
		},
		{
			v1.Descriptor{
				MediaType: schema2.MediaTypeLayer,
				Digest:    digest.Digest("invalid"),
			},
			nil,
			digest.ErrDigestInvalidFormat,
		},
		// normal uploaded gzip layer
		{
			layer,
			nil,
			nil,
		},
		{
			layer,
			[]string{"https://foo/bar"},
			nil,
		},
	}

	for _, c := range layercases {
		m := template
		m.Config = config

		l := c.Desc
		l.URLs = c.URLs

		m.Layers = []v1.Descriptor{l}

		checkFn(m, c.Err)
	}

	configcases := []testcase{
		// valid config
		{
			config,
			nil,
			nil,
		},
		// invalid digest
		{
			v1.Descriptor{
				MediaType: schema2.MediaTypeImageConfig,
			},
			[]string{"https://foo/bar"},
			digest.ErrDigestInvalidFormat,
		},
		{
			v1.Descriptor{
				MediaType: schema2.MediaTypeImageConfig,
				Digest:    digest.Digest("invalid"),
			},
			nil,
			digest.ErrDigestInvalidFormat,
		},
	}

	for _, c := range configcases {
		m := template
		m.Config = c.Desc
		m.Config.URLs = c.URLs

		checkFn(m, c.Err)
	}
}

func TestVerifyManifestConcurrentErrorOrderingAndStress(t *testing.T) {
	ctx := dcontext.Background()
	inmemoryDriver := inmemory.New()
	registry := createRegistry(t, inmemoryDriver,
		EnableValidateImageIndexImagesExist,
	)

	repo := makeRepository(t, registry, "stress-test")
	manifestService := makeManifestService(t, repo)

	config, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeImageConfig, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create 50 missing layers with distinct valid digests so Digest.Validate() succeeds but existence checks fail
	var layers []v1.Descriptor
	for i := 1; i <= 50; i++ {
		dgst := digest.Digest(fmt.Sprintf("sha256:%064d", i))
		layers = append(layers, v1.Descriptor{
			MediaType: schema2.MediaTypeLayer,
			Digest:    dgst,
			Size:      100,
		})
	}

	m := schema2.Manifest{
		Versioned: specs.Versioned{SchemaVersion: 2},
		MediaType: schema2.MediaTypeManifest,
		Config:    config,
		Layers:    layers,
	}

	dm, err := schema2.FromStruct(m)
	if err != nil {
		t.Fatal(err)
	}

	// Run multiple iterations to stress-test concurrent execution and ensure there are no races
	for iter := 0; iter < 20; iter++ {
		_, err = manifestService.Put(ctx, dm)
		if err == nil {
			t.Fatal("expected error putting manifest with missing layers")
		}

		verr, ok := err.(distribution.ErrManifestVerification)
		if !ok {
			t.Fatalf("expected ErrManifestVerification, got %T: %v", err, err)
		}

		// Each of the 50 missing layers should generate an ErrManifestBlobUnknown error in exact original order.
		// Since we have 50 layers, we expect exactly 50 errors in verr.
		if len(verr) != 50 {
			t.Fatalf("expected exactly 50 errors, got %d", len(verr))
		}

		for idx, e := range verr {
			blobUnknownErr, ok := e.(distribution.ErrManifestBlobUnknown)
			if !ok {
				t.Fatalf("expected ErrManifestBlobUnknown, got %T: %v", e, e)
			}
			expectedDgst := digest.Digest(fmt.Sprintf("sha256:%064d", idx+1))
			if blobUnknownErr.Digest != expectedDgst {
				t.Fatalf("error at index %d has wrong digest: expected %s, got %s", idx, expectedDgst, blobUnknownErr.Digest)
			}
		}
	}
}
