package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	dto "github.com/prometheus/client_model/go"
	cacheclient "github.com/tigrisdata/ocache/client"
	"github.com/tigrisdata/tag/cache"
	"github.com/tigrisdata/tag/config"
	"github.com/tigrisdata/tag/metrics"
)

func newWholeObjectCacheService(t *testing.T, objectBody, cacheBody []byte) (*Service, *cache.CachedObjectMeta) {
	t.Helper()

	cfg := config.NewDefault()
	cfg.Cache.SetBlockCachingEnabled(false)
	store := cache.NewCacheWithClient(cacheclient.NewMemoryCache(), &cfg.Cache)
	meta := &cache.CachedObjectMeta{
		Bucket:        "bucket",
		Key:           "key",
		ETag:          `"etag"`,
		ContentType:   "text/plain",
		ContentLength: int64(len(objectBody)),
		StatusCode:    http.StatusOK,
	}
	if err := store.PutWithMeta(context.Background(), meta.Bucket, meta.Key, meta, cacheBody, 0); err != nil {
		t.Fatalf("seed whole-object cache: %v", err)
	}
	return NewService(&mockForwarder{}, store, cfg), meta
}

func requestCountForOperation(t *testing.T, operation, status, source string) float64 {
	t.Helper()

	var metric dto.Metric
	if err := metrics.RequestsTotal.WithLabelValues(operation, status, "unknown", source).Write(&metric); err != nil {
		t.Fatalf("read %s/%s request count: %v", operation, status, err)
	}
	return metric.GetCounter().GetValue()
}

func newTieredShortBodyService(t *testing.T, objectBody, cacheBody []byte) (*Service, *cache.Cache, *atomic.Int64, *cache.CachedObjectMeta) {
	t.Helper()

	forwarder, forwards, _ := tieredMock()
	svc, store := newTieredTestService(forwarder, int64(len(objectBody)+1))
	if w := tieredDo(t, svc, http.MethodPut, "/bucket/key", string(objectBody), nil); w.Code != http.StatusOK {
		t.Fatalf("seed tiered PUT status = %d, want %d", w.Code, http.StatusOK)
	}
	meta, found, err := store.GetMeta(context.Background(), "bucket", "key")
	if err != nil || !found || meta == nil || meta.BodyRef == "" {
		t.Fatalf("seed tiered metadata = %+v, found=%v, err=%v", meta, found, err)
	}
	if err := store.PutBodyStream(context.Background(), "bucket", "key", meta.BodyRef, bytes.NewReader(cacheBody), 0); err != nil {
		t.Fatalf("seed tiered cache body: %v", err)
	}
	return svc, store, forwards, meta
}

func TestHandleGetObject_CacheBodyLength(t *testing.T) {
	smallBody := []byte("small cache body")
	largeBody := bytes.Repeat([]byte("cached body"), 8192)
	proxyCases := []struct {
		name              string
		objectBody        []byte
		cacheBody         []byte
		wantBody          []byte
		wantCacheStatus   string
		wantContentLength string
		wantUpstreamCalls int32
		wantSource        string
		wantRequestStatus string
	}{
		{
			name:              "short-small",
			objectBody:        smallBody,
			cacheBody:         smallBody[:5],
			wantBody:          smallBody,
			wantCacheStatus:   XCacheMiss,
			wantContentLength: strconv.Itoa(len(smallBody)),
			wantUpstreamCalls: 1,
			wantSource:        metrics.SourceUpstream,
			wantRequestStatus: "success",
		},
		{
			name:              "short-large",
			objectBody:        largeBody,
			cacheBody:         largeBody[:32768],
			wantBody:          largeBody[:32768],
			wantCacheStatus:   XCacheHit,
			wantContentLength: strconv.Itoa(len(largeBody)),
			wantSource:        metrics.SourceLocal,
			wantRequestStatus: "error",
		},
		{
			name:              "exact-small",
			objectBody:        smallBody,
			cacheBody:         smallBody,
			wantBody:          smallBody,
			wantCacheStatus:   XCacheHit,
			wantContentLength: strconv.Itoa(len(smallBody)),
			wantSource:        metrics.SourceLocal,
			wantRequestStatus: "success",
		},
		{
			name:              "exact-large",
			objectBody:        largeBody,
			cacheBody:         largeBody,
			wantBody:          largeBody,
			wantCacheStatus:   XCacheHit,
			wantContentLength: strconv.Itoa(len(largeBody)),
			wantSource:        metrics.SourceLocal,
			wantRequestStatus: "success",
		},
		{
			name:              "zero-length",
			wantCacheStatus:   XCacheHit,
			wantContentLength: "0",
			wantSource:        metrics.SourceLocal,
			wantRequestStatus: "success",
		},
	}
	for _, tt := range proxyCases {
		t.Run(tt.name, func(t *testing.T) {
			svc, _ := newWholeObjectCacheService(t, tt.objectBody, tt.cacheBody)
			var upstreamCalls atomic.Int32
			svc.forwarder = &mockForwarder{
				doRequestFunc: func(context.Context, *http.Request, string, string) (*http.Response, error) {
					upstreamCalls.Add(1)
					return &http.Response{
						StatusCode: http.StatusOK,
						Header: http.Header{
							"Content-Length": []string{strconv.Itoa(len(tt.objectBody))},
							"Content-Type":   []string{"text/plain"},
							"ETag":           []string{`"etag"`},
						},
						Body:          io.NopCloser(bytes.NewReader(tt.objectBody)),
						ContentLength: int64(len(tt.objectBody)),
					}, nil
				},
			}

			labels := []struct {
				status string
				source string
			}{
				{status: "success", source: metrics.SourceLocal},
				{status: "error", source: metrics.SourceLocal},
				{status: "success", source: metrics.SourceUpstream},
				{status: "error", source: metrics.SourceUpstream},
			}
			before := make([]float64, len(labels))
			for i, label := range labels {
				before[i] = requestCountForOperation(t, "GetObject", label.status, label.source)
			}

			w := httptest.NewRecorder()
			err := svc.HandleGetObject(w, httptest.NewRequest(http.MethodGet, "/bucket/key", nil))
			if err != nil {
				t.Errorf("body-length property %s: HandleGetObject returned %v, want nil", tt.name, err)
			}
			if w.Code != http.StatusOK {
				t.Errorf("body-length property %s: status = %d, want %d", tt.name, w.Code, http.StatusOK)
			}
			if got := w.Header().Get(XCacheHeader); got != tt.wantCacheStatus {
				t.Errorf("body-length property %s: %s = %q, want %q", tt.name, XCacheHeader, got, tt.wantCacheStatus)
			}
			if got := w.Header().Get("Content-Length"); got != tt.wantContentLength {
				t.Errorf("body-length property %s: Content-Length = %q, want %q", tt.name, got, tt.wantContentLength)
			}
			if !bytes.Equal(w.Body.Bytes(), tt.wantBody) {
				t.Errorf("body-length property %s: returned %d body bytes, want %d", tt.name, w.Body.Len(), len(tt.wantBody))
			}
			if got := upstreamCalls.Load(); got != tt.wantUpstreamCalls {
				t.Errorf("body-length property %s: upstream calls = %d, want %d", tt.name, got, tt.wantUpstreamCalls)
			}
			for i, label := range labels {
				wantDelta := float64(0)
				if label.source == tt.wantSource && label.status == tt.wantRequestStatus {
					wantDelta = 1
				}
				if got := requestCountForOperation(t, "GetObject", label.status, label.source); got != before[i]+wantDelta {
					t.Errorf("body-length property %s: request count %s/%s delta = %v, want %v", tt.name, label.source, label.status, got-before[i], wantDelta)
				}
			}
		})
	}

	tieredCases := []struct {
		name              string
		objectBody        []byte
		cacheBody         []byte
		wantStatus        int
		wantCacheStatus   string
		wantContentLength string
		wantBody          []byte
		wantNoSuchKey     bool
	}{
		{
			name:            "tiered-short-small",
			objectBody:      smallBody,
			cacheBody:       smallBody[:5],
			wantStatus:      http.StatusNotFound,
			wantCacheStatus: XCacheMiss,
			wantNoSuchKey:   true,
		},
		{
			name:              "tiered-short-large",
			objectBody:        largeBody,
			cacheBody:         largeBody[:32768],
			wantStatus:        http.StatusOK,
			wantCacheStatus:   XCacheHit,
			wantContentLength: strconv.Itoa(len(largeBody)),
			wantBody:          largeBody[:32768],
		},
	}
	for _, tt := range tieredCases {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, forwards, _ := newTieredShortBodyService(t, tt.objectBody, tt.cacheBody)
			beforeErrors := requestCountForOperation(t, "GetObject", "error", metrics.SourceLocal)
			beforeSuccesses := requestCountForOperation(t, "GetObject", "success", metrics.SourceLocal)
			beforeForwards := forwards.Load()
			w := httptest.NewRecorder()
			err := svc.HandleGetObject(w, httptest.NewRequest(http.MethodGet, "/bucket/key", nil))
			if err != nil {
				t.Errorf("body-length property %s: HandleGetObject returned %v, want nil", tt.name, err)
			}
			if w.Code != tt.wantStatus {
				t.Errorf("body-length property %s: status = %d, want %d", tt.name, w.Code, tt.wantStatus)
			}
			if got := w.Header().Get(XCacheHeader); got != tt.wantCacheStatus {
				t.Errorf("body-length property %s: %s = %q, want %q", tt.name, XCacheHeader, got, tt.wantCacheStatus)
			}
			if got := w.Header().Get("Content-Length"); got != tt.wantContentLength {
				t.Errorf("body-length property %s: Content-Length = %q, want %q", tt.name, got, tt.wantContentLength)
			}
			if tt.wantNoSuchKey {
				if !bytes.Contains(w.Body.Bytes(), []byte("<Code>NoSuchKey</Code>")) {
					t.Errorf("body-length property %s: response did not contain NoSuchKey: %q", tt.name, w.Body.String())
				}
				if bytes.Contains(w.Body.Bytes(), tt.cacheBody) {
					t.Errorf("body-length property %s: exposed short cache prefix %q", tt.name, tt.cacheBody)
				}
			} else if !bytes.Equal(w.Body.Bytes(), tt.wantBody) {
				t.Errorf("body-length property %s: returned %d body bytes, want %d", tt.name, w.Body.Len(), len(tt.wantBody))
			}
			if got := forwards.Load(); got != beforeForwards {
				t.Errorf("body-length property %s: made %d upstream calls, want none", tt.name, got-beforeForwards)
			}
			if got := requestCountForOperation(t, "GetObject", "error", metrics.SourceLocal); got != beforeErrors+1 {
				t.Errorf("body-length property %s: local error count delta = %v, want 1", tt.name, got-beforeErrors)
			}
			if got := requestCountForOperation(t, "GetObject", "success", metrics.SourceLocal); got != beforeSuccesses {
				t.Errorf("body-length property %s: local success count delta = %v, want 0", tt.name, got-beforeSuccesses)
			}
		})
	}

	t.Run("tiered-head-short", func(t *testing.T) {
		objectBody := []byte("head body for local tier")
		svc, _, forwards, _ := newTieredShortBodyService(t, objectBody, objectBody[:5])
		beforeErrors := requestCountForOperation(t, "HeadObject", "error", metrics.SourceLocal)
		beforeSuccesses := requestCountForOperation(t, "HeadObject", "success", metrics.SourceLocal)
		beforeForwards := forwards.Load()
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodHead, "/bucket/key", nil)
		err := svc.HandleHeadObject(w, r)
		if err != nil {
			t.Errorf("body-length property tiered-head-short: HandleHeadObject returned %v, want nil", err)
		}
		if w.Code != http.StatusNotFound || w.Header().Get(XCacheHeader) != XCacheMiss {
			t.Errorf("body-length property tiered-head-short: response = %d/%q, want 404/MISS", w.Code, w.Header().Get(XCacheHeader))
		}
		if got := requestCountForOperation(t, "HeadObject", "error", metrics.SourceLocal); got != beforeErrors+1 {
			t.Errorf("body-length property tiered-head-short: local error count delta = %v, want 1", got-beforeErrors)
		}
		if got := requestCountForOperation(t, "HeadObject", "success", metrics.SourceLocal); got != beforeSuccesses {
			t.Errorf("body-length property tiered-head-short: local success count delta = %v, want 0", got-beforeSuccesses)
		}
		if got := forwards.Load(); got != beforeForwards {
			t.Errorf("body-length property tiered-head-short: made %d upstream calls, want none", got-beforeForwards)
		}
	})

	t.Run("tiered-conditional-short", func(t *testing.T) {
		objectBody := []byte("conditional body for local tier")
		svc, _, forwards, meta := newTieredShortBodyService(t, objectBody, objectBody[:5])
		beforeErrors := requestCountForOperation(t, "GetObject", "error", metrics.SourceLocal)
		beforeSuccesses := requestCountForOperation(t, "GetObject", "success", metrics.SourceLocal)
		beforeForwards := forwards.Load()
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
		r.Header.Set("If-None-Match", meta.ETag)
		err := svc.HandleGetObject(w, r)
		if err != nil {
			t.Errorf("body-length property tiered-conditional-short: HandleGetObject returned %v, want nil", err)
		}
		if w.Code != http.StatusNotFound || w.Header().Get(XCacheHeader) != XCacheMiss {
			t.Errorf("body-length property tiered-conditional-short: response = %d/%q, want 404/MISS", w.Code, w.Header().Get(XCacheHeader))
		}
		if !bytes.Contains(w.Body.Bytes(), []byte("<Code>NoSuchKey</Code>")) {
			t.Errorf("body-length property tiered-conditional-short: response did not contain NoSuchKey: %q", w.Body.String())
		}
		if got := requestCountForOperation(t, "GetObject", "error", metrics.SourceLocal); got != beforeErrors+1 {
			t.Errorf("body-length property tiered-conditional-short: local error count delta = %v, want 1", got-beforeErrors)
		}
		if got := requestCountForOperation(t, "GetObject", "success", metrics.SourceLocal); got != beforeSuccesses {
			t.Errorf("body-length property tiered-conditional-short: local success count delta = %v, want 0", got-beforeSuccesses)
		}
		if got := forwards.Load(); got != beforeForwards {
			t.Errorf("body-length property tiered-conditional-short: made %d upstream calls, want none", got-beforeForwards)
		}
	})

	t.Run("tiered-head-exact", func(t *testing.T) {
		objectBody := []byte("exact head body")
		svc, _, _, _ := newTieredShortBodyService(t, objectBody, objectBody)
		w := httptest.NewRecorder()
		err := svc.HandleHeadObject(w, httptest.NewRequest(http.MethodHead, "/bucket/key", nil))
		if err != nil || w.Code != http.StatusOK || w.Header().Get("Content-Length") != strconv.Itoa(len(objectBody)) {
			t.Errorf("body-length property tiered-head-exact: response = %d/%q err=%v, want 200 and exact Content-Length", w.Code, w.Header().Get("Content-Length"), err)
		}
	})

	t.Run("tiered-conditional-exact", func(t *testing.T) {
		objectBody := []byte("exact conditional body")
		svc, _, _, meta := newTieredShortBodyService(t, objectBody, objectBody)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
		r.Header.Set("If-None-Match", meta.ETag)
		err := svc.HandleGetObject(w, r)
		if err != nil || w.Code != http.StatusNotModified {
			t.Errorf("body-length property tiered-conditional-exact: response = %d err=%v, want 304", w.Code, err)
		}
	})

	t.Run("tiered-ifmatch-short", func(t *testing.T) {
		objectBody := []byte("if-match body")
		svc, _, _, _ := newTieredShortBodyService(t, objectBody, objectBody[:5])
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
		r.Header.Set("If-Match", `"not-the-etag"`)
		err := svc.HandleGetObject(w, r)
		if err != nil || w.Code != http.StatusPreconditionFailed {
			t.Errorf("body-length property tiered-ifmatch-short: response = %d err=%v, want 412", w.Code, err)
		}
		if !bytes.Contains(w.Body.Bytes(), []byte("<Code>PreconditionFailed</Code>")) {
			t.Errorf("body-length property tiered-ifmatch-short: response did not preserve PreconditionFailed: %q", w.Body.String())
		}
	})
}
