package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

// parquetPresenceDelayClient is a controlled latency model for the two-byte
// presence request. The underlying memory cache still supplies the real
// BlockExists result; this benchmark isolates the wait that overlaps remotely.
type parquetPresenceDelayClient struct {
	cacheclient.CacheClient
	delay time.Duration
}

func (c *parquetPresenceDelayClient) IsLocal(string) bool { return false }

func (c *parquetPresenceDelayClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if strings.HasPrefix(key, "blk|") && start == 0 && end == 1 && c.delay > 0 {
		timer := time.NewTimer(c.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

// BenchmarkParquetFooterPresenceScan measures the shared footer block selection
// path with all non-tail blocks present. It isolates wait overlap with a controlled
// delay and memory cache; it is not a full S3-open or remote-backend benchmark.
func BenchmarkParquetFooterPresenceScan(b *testing.B) {
	const (
		bucket = "benchmark"
		key    = "footer.parquet"
		etag   = `"v1"`
	)
	for _, footerBlocks := range []int{2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("blocks-%d", footerBlocks), func(b *testing.B) {
			blockSize := int64(config.DefaultCacheBlockSize)
			footerLen := int64(footerBlocks) * blockSize
			meta := &cache.CachedObjectMeta{
				Bucket:        bucket,
				Key:           key,
				ETag:          etag,
				ContentLength: footerLen * 80,
				BlockSize:     blockSize,
			}
			blocks := parquetFooterBlocks(meta, footerLen, true)
			if len(blocks) != footerBlocks {
				b.Fatalf("footer blocks = %d, want %d", len(blocks), footerBlocks)
			}

			cfg := config.NewDefault()
			cfg.Cache.SetBlockCachingEnabled(true)
			cfg.Cache.BlockSize = blockSize
			cfg.Cache.ParquetOptimization = true
			underlying := cacheclient.NewMemoryCache()
			store := cache.NewCacheWithClient(&parquetPresenceDelayClient{
				CacheClient: underlying,
				delay:       2 * time.Millisecond,
			}, &cfg.Cache)
			for _, idx := range blocks {
				if err := store.PutBlock(context.Background(), bucket, key, etag, blockSize, idx, make([]byte, blockSize), 3600); err != nil {
					b.Fatalf("seed block %d: %v", idx, err)
				}
			}
			trailer := make([]byte, parquetTrailerSize)
			binary.LittleEndian.PutUint32(trailer, uint32(footerLen))
			copy(trailer[4:], parquetMagic)
			service := NewService(nil, store, cfg)

			b.ResetTimer()
			for b.Loop() {
				if !service.prefetchParquetFooterBlocks(bucket, key, "access", "secret", meta, trailer) {
					b.Fatal("footer presence scan did not complete")
				}
			}
		})
	}
}
