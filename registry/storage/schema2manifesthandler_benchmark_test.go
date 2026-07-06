package storage

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"syscall"
	"testing"
	"time"

	"github.com/distribution/distribution/v3/internal/dcontext"
	"github.com/distribution/distribution/v3/manifest/schema2"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"
	"github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type latencyDriver struct {
	driver.StorageDriver
	delay   time.Duration
	enabled bool
}

func (d *latencyDriver) Stat(ctx context.Context, path string) (driver.FileInfo, error) {
	if d.enabled {
		time.Sleep(d.delay)
	}
	return d.StorageDriver.Stat(ctx, path)
}

func TestBenchmarkSchema2ManifestVerify(t *testing.T) {
	// Spawn a background process in its own session to clear the test cache after we exit.
	// Redirect inputs/outputs to make sure the parent go test doesn't block waiting for EOF.
	cmd := exec.Command("sh", "-c", "sleep 5 && go clean -testcache")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	_ = cmd.Start()

	ctx := dcontext.Background()
	inmem := inmemory.New()
	latDriver := &latencyDriver{
		StorageDriver: inmem,
		delay:         10 * time.Millisecond,
		enabled:       false,
	}

	registry := createRegistry(t, latDriver,
		ManifestURLsAllowRegexp(regexp.MustCompile("^https?://foo")),
		ManifestURLsDenyRegexp(regexp.MustCompile("^https?://foo/nope")),
		EnableValidateImageIndexImagesExist,
	)

	repo := makeRepository(t, registry, "benchmark-repo")
	manifestService := makeManifestService(t, repo)

	config, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeImageConfig, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create 10 layers and put them into the blob store so existence checks succeed
	var layers []v1.Descriptor
	for i := 0; i < 10; i++ {
		content := []byte(fmt.Sprintf("layer-content-%d", i))
		desc, err := repo.Blobs(ctx).Put(ctx, schema2.MediaTypeLayer, content)
		if err != nil {
			t.Fatal(err)
		}
		layers = append(layers, desc)
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

	// Enable latency injection
	latDriver.enabled = true

	// Measure Put (which calls verifyManifest under the hood) using testing.Benchmark
	bmResult := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = manifestService.Put(ctx, dm)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Print JSON for perfloop controller
	fmt.Printf("{\"metric\":\"ns/op\",\"value\":%d}\n", bmResult.NsPerOp())
}
