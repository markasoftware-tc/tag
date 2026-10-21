package proxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
)

type parquetProbeTestClient struct {
	cacheclient.CacheClient

	release    <-chan struct{}
	started    chan<- string
	local      bool
	errorKey   map[string]error
	errorGates map[string]<-chan struct{}

	mu     sync.Mutex
	calls  int
	active int
	peak   int
}

func (c *parquetProbeTestClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	if !strings.HasPrefix(key, "blk|") || start != 0 || end != 1 {
		return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
	}

	c.mu.Lock()
	c.calls++
	c.active++
	if c.active > c.peak {
		c.peak = c.active
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()

	if c.started != nil {
		c.started <- key
	}
	if err := c.errorKey[key]; err != nil {
		if gate := c.errorGates[key]; gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return err
	}
	if c.release != nil {
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func (c *parquetProbeTestClient) IsLocal(string) bool { return c.local }

func (c *parquetProbeTestClient) snapshot() (calls, active, peak int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.active, c.peak
}

func newParquetProbeTestService(t *testing.T, client cacheclient.CacheClient) (*Service, *cache.Cache, int64) {
	t.Helper()
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = 16
	cfg.Cache.ParquetOptimization = true
	store := cache.NewCacheWithClient(client, &cfg.Cache)
	return NewService(nil, store, cfg), store, 16
}

func parquetProbeTestMeta(key string, blockSize int64, footerBlocks int) (*cache.CachedObjectMeta, []int64, int64) {
	footerLen := int64(footerBlocks) * blockSize
	meta := &cache.CachedObjectMeta{
		Bucket:        "probe-bucket",
		Key:           key,
		ETag:          `"v1"`,
		ContentLength: footerLen * 80,
		BlockSize:     blockSize,
	}
	blocks := parquetFooterBlocks(meta, footerLen, true)
	return meta, blocks, footerLen
}

func seedParquetProbeBlocks(t *testing.T, store *cache.Cache, meta *cache.CachedObjectMeta, blocks []int64) {
	t.Helper()
	for _, idx := range blocks {
		if err := store.PutBlock(context.Background(), meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, idx, make([]byte, meta.BlockSize), 3600); err != nil {
			t.Fatalf("seed block %d: %v", idx, err)
		}
	}
}

func waitParquetProbeStarts(t *testing.T, started <-chan string, count int) []string {
	t.Helper()
	keys := make([]string, 0, count)
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for len(keys) < count {
		select {
		case key := <-started:
			keys = append(keys, key)
		case <-timer.C:
			t.Fatalf("got %d of %d presence probes", len(keys), count)
		}
	}
	return keys
}

type unknownLocalityParquetProbeClient struct {
	cacheclient.CacheClient
}

func (c *unknownLocalityParquetProbeClient) GetRangeStream(ctx context.Context, key string, start, end int64, w io.Writer) error {
	return c.CacheClient.GetRangeStream(ctx, key, start, end, w)
}

func waitParquetProbeClientIdle(t *testing.T, client *parquetProbeTestClient, service *Service) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		calls, active, _ := client.snapshot()
		if active == 0 && len(service.parquetFooterProbeSlots) == 0 {
			return calls
		}
		time.Sleep(time.Millisecond)
	}
	calls, active, _ := client.snapshot()
	t.Fatalf("presence probes did not drain: calls=%d active=%d slots=%d", calls, active, len(service.parquetFooterProbeSlots))
	return 0
}

func TestReadFooterPresenceProbesStaySerialForLocalOrUnknownOwners(t *testing.T) {
	for _, tc := range []struct {
		name    string
		local   bool
		unknown bool
	}{
		{name: "local", local: true},
		{name: "unknown locality", unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseProbes := func() { releaseOnce.Do(func() { close(release) }) }
			started := make(chan string, 8)
			probe := &parquetProbeTestClient{
				CacheClient: cacheclient.NewMemoryCache(),
				release:     release,
				started:     started,
				local:       tc.local,
				errorKey:    make(map[string]error),
			}
			var client cacheclient.CacheClient = probe
			if tc.unknown {
				client = &unknownLocalityParquetProbeClient{CacheClient: probe}
			}
			service, store, blockSize := newParquetProbeTestService(t, client)
			meta, blocks, footerLen := parquetProbeTestMeta("locality.parquet", blockSize, 4)
			seedParquetProbeBlocks(t, store, meta, blocks)
			t.Cleanup(func() {
				releaseProbes()
				waitParquetProbeClientIdle(t, probe, service)
			})

			result := make(chan bool, 1)
			go func() {
				result <- service.ensureParquetFooterBlocks(context.Background(), meta.Bucket, meta.Key, "access", "secret", meta, footerLen, true, triggerReadPrefetch)
			}()
			waitParquetProbeStarts(t, started, 1)
			select {
			case key := <-started:
				t.Fatalf("second local/unknown presence probe started in parallel: %s", key)
			case <-time.After(50 * time.Millisecond):
			}
			releaseProbes()
			if ok := <-result; !ok {
				t.Fatal("serial read-triggered scan did not complete")
			}
			if calls, active, peak := probe.snapshot(); calls != len(blocks) || active != 0 || peak != 1 {
				t.Fatalf("locality probe stats = (calls=%d active=%d peak=%d), want (%d, 0, 1)", calls, active, peak, len(blocks))
			}
		})
	}
}

func TestParquetFooterProbeRebuildsAbsentBlocksInInputOrder(t *testing.T) {
	base := cacheclient.NewMemoryCache()
	probe := &parquetProbeTestClient{
		CacheClient: base,
		errorKey:    make(map[string]error),
	}
	service, store, blockSize := newParquetProbeTestService(t, probe)
	blockCount := maxConcurrentParquetFooterProbeWorkers*2 + 1
	meta, blocks, _ := parquetProbeTestMeta("ordered.parquet", blockSize, blockCount)

	var want []int64
	for i, idx := range blocks {
		if i == 1 || i == 4 {
			want = append(want, idx)
			continue
		}
		seedParquetProbeBlocks(t, store, meta, []int64{idx})
	}
	// A transient cache read error has the same historical false result as an
	// absent block, but it must occupy its original position in the fetch list.
	errorIndex := blocks[3]
	probe.errorKey[cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, errorIndex)] = errors.New("probe failed")
	want = []int64{blocks[1], errorIndex, blocks[4]}

	got := service.missingParquetFooterBlocks(context.Background(), meta.Bucket, meta.Key, meta, blocks, true)
	if !slices.Equal(got, want) {
		t.Fatalf("absent blocks = %v, want input-ordered %v", got, want)
	}
	if calls, active, _ := probe.snapshot(); calls != len(blocks) || active != 0 {
		t.Fatalf("presence calls = %d active=%d, want %d completed probes", calls, active, len(blocks))
	}
}

func TestEnsureBlocksCachedParallelFooterProbesRetainFirstError(t *testing.T) {
	base := cacheclient.NewMemoryCache()
	firstError := errors.New("first transient probe failure")
	laterError := errors.New("later transient probe failure")
	probe := &parquetProbeTestClient{
		CacheClient: base,
		errorKey:    make(map[string]error),
	}
	service, _, blockSize := newParquetProbeTestService(t, probe)
	meta, footer, _ := parquetProbeTestMeta("foreground.parquet", blockSize, 8)
	tailBlock := (meta.ContentLength - 1) / meta.BlockSize
	blocks := make([]int64, 0, len(footer)+1)
	blocks = append(blocks, footer...)
	blocks = append(blocks, tailBlock)
	probe.errorKey[cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, blocks[1])] = firstError
	probe.errorKey[cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, blocks[len(blocks)-2])] = laterError
	service.activeBackgroundFetches.Store("pq:"+meta.Bucket+"/"+meta.Key+"/"+meta.ETag, struct{}{})

	err := service.ensureBlocksCached(context.Background(), meta.Bucket, meta.Key, "access", "secret", meta, blocks[0], tailBlock, false, maxRangeBlockFanout)
	if !errors.Is(err, firstError) {
		t.Fatalf("ensureBlocksCached error = %v, want first input-ordered probe error", err)
	}
	calls := waitParquetProbeClientIdle(t, probe, service)
	if calls == 0 || calls > len(blocks) {
		t.Fatalf("foreground presence calls = %d, want a bounded prefix of %d probes", calls, len(blocks))
	}
}

func TestEnsureBlocksCachedParallelProbeErrorCancelsLaterWaits(t *testing.T) {
	base := cacheclient.NewMemoryCache()
	releaseLater := make(chan struct{})
	releaseFirstError := make(chan struct{})
	firstError := errors.New("first transient probe failure")
	started := make(chan string, 16)
	probe := &parquetProbeTestClient{
		CacheClient: base,
		release:     releaseLater,
		started:     started,
		errorKey:    make(map[string]error),
		errorGates:  make(map[string]<-chan struct{}),
	}
	service, _, blockSize := newParquetProbeTestService(t, probe)
	var releaseLaterOnce, releaseFirstOnce sync.Once
	releaseLaterProbes := func() { releaseLaterOnce.Do(func() { close(releaseLater) }) }
	releaseFirst := func() { releaseFirstOnce.Do(func() { close(releaseFirstError) }) }
	t.Cleanup(func() {
		releaseFirst()
		releaseLaterProbes()
		waitParquetProbeClientIdle(t, probe, service)
	})
	meta, footer, _ := parquetProbeTestMeta("stalled-foreground.parquet", blockSize, 8)
	tailBlock := (meta.ContentLength - 1) / meta.BlockSize
	blocks := append(append([]int64(nil), footer...), tailBlock)
	firstKey := cache.MakeBlockKey(meta.Bucket, meta.Key, meta.ETag, meta.BlockSize, blocks[0])
	probe.errorKey[firstKey] = firstError
	probe.errorGates[firstKey] = releaseFirstError
	service.activeBackgroundFetches.Store("pq:"+meta.Bucket+"/"+meta.Key+"/"+meta.ETag, struct{}{})

	ctx := context.Background()
	result := make(chan error, 1)
	go func() {
		result <- service.ensureBlocksCached(ctx, meta.Bucket, meta.Key, "access", "secret", meta, blocks[0], tailBlock, false, maxRangeBlockFanout)
	}()
	waitParquetProbeStarts(t, started, maxConcurrentParquetFooterProbeWorkers)
	releaseFirst()

	var err error
	select {
	case err = <-result:
	case <-time.After(time.Second):
		releaseLaterProbes()
		err = <-result
		calls := waitParquetProbeClientIdle(t, probe, service)
		t.Fatalf("foreground returned only after releasing later probes: err=%v calls=%d", err, calls)
	}
	releaseLaterProbes()
	if !errors.Is(err, firstError) {
		t.Fatalf("ensureBlocksCached error = %v, want first ordered probe error", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("helper canceled caller context: %v", ctx.Err())
	}
	if calls := waitParquetProbeClientIdle(t, probe, service); calls > len(blocks) {
		t.Fatalf("foreground presence calls = %d, want at most %d", calls, len(blocks))
	}
}

func TestHandleGetObjectParquetRangeReturnsAfterFirstProbeError(t *testing.T) {
	const (
		bucket     = "probe-bucket"
		key        = "stalled.parquet"
		etag       = `"v1"`
		blockSize  = int64(16)
		footerSize = int64(8 * blockSize)
	)
	objectSize := footerSize * 80
	body := make([]byte, objectSize)
	binary.LittleEndian.PutUint32(body[objectSize-parquetTrailerSize:], uint32(footerSize))
	copy(body[objectSize-4:], parquetMagic)
	origin := newBlockMock(body, etag)

	releaseLater := make(chan struct{})
	releaseFirstError := make(chan struct{})
	firstError := errors.New("first transient probe failure")
	started := make(chan string, 32)
	probe := &parquetProbeTestClient{
		CacheClient: cacheclient.NewMemoryCache(),
		release:     releaseLater,
		started:     started,
		errorKey:    make(map[string]error),
		errorGates:  make(map[string]<-chan struct{}),
	}
	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(true)
	cfg.Cache.BlockSize = blockSize
	cfg.Cache.SizeThreshold = objectSize * 2
	cfg.Cache.ParquetOptimization = true
	store := cache.NewCacheWithClient(probe, &cfg.Cache)
	service := NewService(origin, store, cfg)
	meta := &cache.CachedObjectMeta{
		Bucket:        bucket,
		Key:           key,
		ETag:          etag,
		ContentLength: objectSize,
		BlockSize:     blockSize,
		StatusCode:    http.StatusOK,
		ContentType:   "application/octet-stream",
	}
	if wrote, err := store.PutMetaIfVersion(context.Background(), bucket, key, meta, 3600, cache.VersionAny); err != nil || !wrote {
		t.Fatalf("seed metadata = (wrote=%t, err=%v)", wrote, err)
	}

	footerStart := objectSize - parquetTrailerSize - footerSize
	firstBlock, _ := coveringBlocks(footerStart, objectSize-parquetTrailerSize-1, blockSize)
	firstKey := cache.MakeBlockKey(bucket, key, etag, blockSize, firstBlock)
	probe.errorKey[firstKey] = firstError
	probe.errorGates[firstKey] = releaseFirstError
	versionKey := "pq:" + bucket + "/" + key + "/" + etag
	service.activeBackgroundFetches.Store(versionKey, struct{}{})
	service.parquetFooterPrefetchSlots <- struct{}{}
	var releaseFirstOnce, releaseLaterOnce, releaseScanOnce sync.Once
	releaseFirst := func() { releaseFirstOnce.Do(func() { close(releaseFirstError) }) }
	releaseLaterProbes := func() { releaseLaterOnce.Do(func() { close(releaseLater) }) }
	releaseScan := func() {
		releaseScanOnce.Do(func() {
			service.activeBackgroundFetches.Delete(versionKey)
			<-service.parquetFooterPrefetchSlots
		})
	}
	t.Cleanup(func() {
		releaseFirst()
		releaseLaterProbes()
		releaseScan()
		waitParquetProbeClientIdle(t, probe, service)
	})

	request := blockGet(bucket, key, fmt.Sprintf("bytes=%d-%d", footerStart, objectSize-parquetTrailerSize-1))
	response := httptest.NewRecorder()
	result := make(chan error, 1)
	go func() { result <- service.HandleGetObject(response, request) }()
	waitParquetProbeStarts(t, started, maxConcurrentParquetFooterProbeWorkers)
	releaseFirst()

	var err error
	select {
	case err = <-result:
	case <-time.After(time.Second):
		releaseLaterProbes()
		err = <-result
		releaseScan()
		waitParquetProbeClientIdle(t, probe, service)
		t.Fatalf("foreground Range did not fall through until later probes were released: %v", err)
	}
	releaseLaterProbes()
	releaseScan()
	if err != nil {
		t.Fatalf("HandleGetObject: %v", err)
	}
	if response.Code != http.StatusPartialContent {
		t.Fatalf("foreground Range status = %d, want %d", response.Code, http.StatusPartialContent)
	}
	if !slices.Equal(response.Body.Bytes(), body[footerStart:objectSize-parquetTrailerSize]) {
		t.Fatalf("foreground Range body = %d bytes, want %d", response.Body.Len(), footerSize)
	}
	if got := response.Header().Get("Content-Range"); got != fmt.Sprintf("bytes %d-%d/%d", footerStart, objectSize-parquetTrailerSize-1, objectSize) {
		t.Fatalf("foreground Range Content-Range = %q", got)
	}
	if got := origin.forwards.Load(); got != 1 {
		t.Fatalf("upstream Range forwards = %d, want 1", got)
	}
	waitParquetProbeClientIdle(t, probe, service)
}

func TestEnsureBlocksCachedOverlappingFooterWarmUsesBoundedProbes(t *testing.T) {
	base := cacheclient.NewMemoryCache()
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseProbes := func() { releaseOnce.Do(func() { close(release) }) }
	started := make(chan string, maxConcurrentParquetFooterProbeWorkers+1)
	probe := &parquetProbeTestClient{CacheClient: base, release: release, started: started}
	service, store, blockSize := newParquetProbeTestService(t, probe)
	t.Cleanup(releaseProbes)
	meta, footer, _ := parquetProbeTestMeta("foreground.parquet", blockSize, 2)
	tailBlock := (meta.ContentLength - 1) / meta.BlockSize
	rangeBlocks := make([]int64, 0, 3)
	rangeBlocks = append(rangeBlocks, footer...)
	rangeBlocks = append(rangeBlocks, tailBlock)
	seedParquetProbeBlocks(t, store, meta, rangeBlocks)
	service.activeBackgroundFetches.Store("pq:"+meta.Bucket+"/"+meta.Key+"/"+meta.ETag, struct{}{})

	result := make(chan error, 1)
	go func() {
		result <- service.ensureBlocksCached(context.Background(), meta.Bucket, meta.Key, "access", "secret", meta, rangeBlocks[0], tailBlock, false, maxRangeBlockFanout)
	}()
	waitParquetProbeStarts(t, started, 2)
	releaseProbes()
	if err := <-result; err != nil {
		t.Fatalf("foreground Parquet range preflight: %v", err)
	}
	if calls, active, peak := probe.snapshot(); calls != len(rangeBlocks) || active != 0 || peak < 2 || peak > maxConcurrentParquetFooterProbeWorkers {
		t.Fatalf("foreground footer probes = (calls=%d active=%d peak=%d), want %d ordered probes and peak in [2,%d]", calls, active, peak, len(rangeBlocks), maxConcurrentParquetFooterProbeWorkers)
	}
}

func TestMaybePrefetchParquetFooter_OverlapsBoundedPresenceProbes(t *testing.T) {
	fixture := newParquetAdmissionFixture(t)
	defer fixture.release()
	key := "parallel-footer.parquet"
	meta := fixture.seedObject(t, key)
	start := meta.ContentLength - parquetTrailerSize
	trailer := fixture.body[start:]

	fixture.service.maybePrefetchParquetFooter("b", key, "access", "secret", meta, start, meta.ContentLength-1, trailer)
	started := waitForAdmissionProbeStarts(t, fixture.started, 2)
	blocks := parquetFooterBlocks(meta, admissionTestFooterLen, true)
	wantKeys := make([]string, 0, len(blocks))
	for _, idx := range blocks {
		wantKeys = append(wantKeys, cache.MakeBlockKey("b", key, meta.ETag, meta.BlockSize, idx))
	}
	slices.Sort(started)
	slices.Sort(wantKeys)
	if !slices.Equal(started, wantKeys) {
		t.Fatalf("initial presence probes = %v, want %v", started, wantKeys)
	}

	fixture.release()
	waitForBackgroundFetches(t, fixture.service)
	if got := fixture.client.peak; got < 2 || got > maxConcurrentParquetFooterProbeWorkers {
		t.Fatalf("read-trigger probe peak = %d, want [2,%d]", got, maxConcurrentParquetFooterProbeWorkers)
	}
	forwarder, ok := fixture.service.forwarder.(*admissionFooterForwarder)
	if !ok {
		t.Fatalf("forwarder type = %T, want *admissionFooterForwarder", fixture.service.forwarder)
	}
	gotRanges := forwarder.requestedRanges()
	slices.Sort(gotRanges)
	wantRanges := make([]string, 0, len(blocks))
	for _, idx := range blocks {
		start, end := blockBounds(idx, meta.BlockSize, meta.ContentLength)
		wantRanges = append(wantRanges, fmt.Sprintf("bytes=%d-%d", start, end))
	}
	slices.Sort(wantRanges)
	if !slices.Equal(gotRanges, wantRanges) {
		t.Fatalf("read-trigger upstream ranges = %v, want %v", gotRanges, wantRanges)
	}
	for _, idx := range blocks {
		if !fixture.store.BlockExists(context.Background(), "b", key, meta.ETag, meta.BlockSize, idx) {
			t.Fatalf("footer block %d is absent after prefetch completion", idx)
		}
	}
}

func TestParquetFooterProbeServiceCapCoversOverlappingForegroundRanges(t *testing.T) {
	base := cacheclient.NewMemoryCache()
	footerBlockCount := 2
	rangeBlockCount := footerBlockCount + 1
	workersPerRange := min(rangeBlockCount, maxConcurrentParquetFooterProbeWorkers)
	release := make(chan struct{})
	foregroundCount := maxConcurrentParquetFooterProbeCalls/workersPerRange + 2
	totalProbes := foregroundCount * rangeBlockCount
	started := make(chan string, totalProbes)
	probe := &parquetProbeTestClient{CacheClient: base, release: release, started: started}
	service, store, blockSize := newParquetProbeTestService(t, probe)
	meta, footerBlocks, _ := parquetProbeTestMeta("parallel.parquet", blockSize, footerBlockCount)
	tailBlock := (meta.ContentLength - 1) / meta.BlockSize
	rangeBlocks := append(append([]int64(nil), footerBlocks...), tailBlock)
	seedParquetProbeBlocks(t, store, meta, rangeBlocks)
	service.parquetFooterPrefetchSlots <- struct{}{} // an admitted footer scan is active
	service.activeBackgroundFetches.Store("pq:"+meta.Bucket+"/"+meta.Key+"/"+meta.ETag, struct{}{})

	start := make(chan struct{})
	ready := make(chan struct{}, foregroundCount)
	results := make(chan error, foregroundCount)
	var group sync.WaitGroup
	group.Add(foregroundCount)
	for request := range foregroundCount {
		go func(i int) {
			defer group.Done()
			ready <- struct{}{}
			<-start
			err := service.ensureBlocksCached(context.Background(), meta.Bucket, meta.Key, "access", "secret", meta, rangeBlocks[0], tailBlock, false, maxRangeBlockFanout)
			if err != nil {
				results <- fmt.Errorf("foreground range %d: %w", i, err)
				return
			}
			results <- nil
		}(request)
	}
	for range foregroundCount {
		<-ready
	}
	close(start)
	waitParquetProbeStarts(t, started, maxConcurrentParquetFooterProbeCalls)
	if got := len(service.parquetFooterProbeSlots); got != maxConcurrentParquetFooterProbeCalls {
		t.Fatalf("active service probe slots = %d, want cap %d", got, maxConcurrentParquetFooterProbeCalls)
	}

	overCap := false
	timer := time.NewTimer(time.Second)
	select {
	case <-started:
		overCap = true
	case <-timer.C:
	}
	timer.Stop()
	close(release)
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if overCap {
		t.Fatal("a 33rd foreground presence call started while all service probe slots were occupied")
	}
	if calls, active, peak := probe.snapshot(); calls != totalProbes || active != 0 || peak != maxConcurrentParquetFooterProbeCalls {
		t.Fatalf("service probe stats = (calls=%d active=%d peak=%d), want (%d, 0, %d)", calls, active, peak, totalProbes, maxConcurrentParquetFooterProbeCalls)
	}
}

func TestParquetFooterProbeCancellationReleasesSlots(t *testing.T) {
	base := cacheclient.NewMemoryCache()
	release := make(chan struct{})
	blockCount := maxConcurrentParquetFooterProbeWorkers * 2
	started := make(chan string, blockCount)
	probe := &parquetProbeTestClient{CacheClient: base, release: release, started: started}
	service, store, blockSize := newParquetProbeTestService(t, probe)
	meta, blocks, _ := parquetProbeTestMeta("cancel.parquet", blockSize, blockCount)
	seedParquetProbeBlocks(t, store, meta, blocks)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan []int64, 1)
	go func() {
		result <- service.missingParquetFooterBlocks(ctx, meta.Bucket, meta.Key, meta, blocks, true)
	}()
	waitParquetProbeStarts(t, started, 2)
	cancel()
	select {
	case absent := <-result:
		if !slices.Equal(absent, blocks) {
			t.Fatalf("cancelled scan absent blocks = %v, want all %v", absent, blocks)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled scan did not release its probe workers")
	}
	if calls, active, _ := probe.snapshot(); calls < 2 || active != 0 || len(service.parquetFooterProbeSlots) != 0 {
		t.Fatalf("cancelled scan state = (calls=%d active=%d slots=%d), want completed probes and no held slots", calls, active, len(service.parquetFooterProbeSlots))
	}
}
