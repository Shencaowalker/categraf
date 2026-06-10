package lokimtail

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/types"
)

func TestInstanceGatherAccumulatesAndPersistsCursor(t *testing.T) {
	now := time.Unix(1700000000, 0)
	ts1 := now.Add(-2 * time.Minute).UnixNano()
	ts2 := now.Add(-90 * time.Second).UnixNano()

	resp := map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "streams",
			"result": []any{
				map[string]any{
					"stream": map[string]string{
						"job":      "app",
						"filename": "/data/logs/app.log",
					},
					"values": []any{
						[]string{int64ToString(ts1), "INFO trace=t1 api=/pay cost=12"},
						[]string{int64ToString(ts2), "INFO trace=t2 api=/refund cost=7"},
					},
				},
			},
		},
	}
	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	stateFile := filepath.Join(t.TempDir(), "state.json")
	ins := &Instance{
		URL:           "http://loki.example.com",
		Timeout:       config.Duration(2 * time.Second),
		IngestDelay:   config.Duration(30 * time.Second),
		Lookback:      config.Duration(5 * time.Minute),
		Overlap:       config.Duration(5 * time.Second),
		Limit:         100,
		StateFile:     stateFile,
		MaxDedupItems: 128,
		StaticLabels:  map[string]string{"service": "billing"},
		nowFn:         func() time.Time { return now },
		HTTPCommonConfig: config.HTTPCommonConfig{
			Method: "GET",
		},
		Rules: []*Rule{
			{
				Name:         "requests",
				Selector:     `{job="app"}`,
				Regex:        `trace=(\w+) api=(\S+) cost=(\d+)`,
				LabelNames:   []string{"trace", "api", "cost"},
				Metric:       "app_requests_total",
				MetricLabels: []string{"api"},
				Value:        1,
			},
			{
				Name:         "cost",
				Selector:     `{job="app"}`,
				Regex:        `trace=(\w+) api=(\S+) cost=(\d+)`,
				LabelNames:   []string{"trace", "api", "cost"},
				Metric:       "app_request_cost_total",
				MetricLabels: []string{"api"},
				ValueFrom:    "cost",
			},
		},
	}

	if err := ins.Init(); err != nil {
		t.Fatalf("init instance: %v", err)
	}
	ins.client = fakeClient{body: body}

	slist := types.NewSampleList()
	ins.Gather(slist)
	samples := slist.PopBackAll()
	if len(samples) != 4 {
		t.Fatalf("expected 4 samples after first gather, got %d", len(samples))
	}

	assertSampleValue(t, samples, "lokimtail_app_requests_total", map[string]string{
		"service": "billing",
		"api":     "/pay",
	}, 1)
	assertSampleValue(t, samples, "lokimtail_app_requests_total", map[string]string{
		"service": "billing",
		"api":     "/refund",
	}, 1)
	assertSampleValue(t, samples, "lokimtail_app_request_cost_total", map[string]string{
		"service": "billing",
		"api":     "/pay",
	}, 12)
	assertSampleValue(t, samples, "lokimtail_app_request_cost_total", map[string]string{
		"service": "billing",
		"api":     "/refund",
	}, 7)

	slist = types.NewSampleList()
	ins.Gather(slist)
	samples = slist.PopBackAll()
	if len(samples) != 4 {
		t.Fatalf("expected 4 samples after second gather, got %d", len(samples))
	}
	assertSampleValue(t, samples, "lokimtail_app_requests_total", map[string]string{
		"service": "billing",
		"api":     "/pay",
	}, 1)
	assertSampleValue(t, samples, "lokimtail_app_request_cost_total", map[string]string{
		"service": "billing",
		"api":     "/refund",
	}, 7)

	st, err := loadState(stateFile)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if st.Rules["requests"].CursorTs != ts2 {
		t.Fatalf("expected requests cursor %d, got %d", ts2, st.Rules["requests"].CursorTs)
	}
}

func TestInstanceGatherCapsRangeFromOldCursor(t *testing.T) {
	now := time.Unix(1700000000, 0)
	oldCursor := now.Add(-757 * time.Hour).UnixNano()
	maxQueryRange := 720 * time.Hour
	stateFile := filepath.Join(t.TempDir(), "state.json")

	st := &pluginState{
		Version: 1,
		Rules: map[string]*ruleState{
			"requests": {
				CursorTs: oldCursor,
				Counters: map[string]float64{},
				Recent:   map[string]int64{},
			},
		},
	}
	if err := saveState(stateFile, st); err != nil {
		t.Fatalf("save state: %v", err)
	}

	client := &captureClient{body: []byte(`{"status":"success","data":{"resultType":"streams","result":[]}}`)}
	ins := &Instance{
		URL:           "http://loki.example.com",
		Timeout:       config.Duration(2 * time.Second),
		IngestDelay:   0,
		Lookback:      config.Duration(5 * time.Minute),
		Overlap:       0,
		Limit:         100,
		StateFile:     stateFile,
		MaxQueryRange: config.Duration(maxQueryRange),
		nowFn:         func() time.Time { return now },
		HTTPCommonConfig: config.HTTPCommonConfig{
			Method: "GET",
		},
		Rules: []*Rule{
			{
				Name:       "requests",
				Selector:   `{job="app"}`,
				Regex:      `api=(\S+)`,
				LabelNames: []string{"api"},
				Metric:     "app_requests_total",
				Value:      1,
			},
		},
	}

	if err := ins.Init(); err != nil {
		t.Fatalf("init instance: %v", err)
	}
	ins.client = client

	ins.Gather(types.NewSampleList())

	if client.lastRequest == nil {
		t.Fatal("expected loki query request")
	}
	query := client.lastRequest.URL.Query()
	start, err := strconv.ParseInt(query.Get("start"), 10, 64)
	if err != nil {
		t.Fatalf("parse start: %v", err)
	}
	end, err := strconv.ParseInt(query.Get("end"), 10, 64)
	if err != nil {
		t.Fatalf("parse end: %v", err)
	}
	if start != oldCursor {
		t.Fatalf("expected start %d, got %d", oldCursor, start)
	}
	expectedEnd := time.Unix(0, oldCursor).Add(maxQueryRange).UnixNano()
	if end != expectedEnd {
		t.Fatalf("expected capped end %d, got %d", expectedEnd, end)
	}

	gotState, err := loadState(stateFile)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if gotState.Rules["requests"].CursorTs != expectedEnd {
		t.Fatalf("expected cursor to advance to capped end %d, got %d", expectedEnd, gotState.Rules["requests"].CursorTs)
	}
}

func TestInstanceInitRejectsDuplicateRuleNames(t *testing.T) {
	ins := &Instance{
		URL:       "http://loki.example.com",
		StateFile: filepath.Join(t.TempDir(), "state.json"),
		HTTPCommonConfig: config.HTTPCommonConfig{
			Method: "GET",
		},
		Rules: []*Rule{
			{
				Name:       "requests",
				Selector:   `{job="app"}`,
				Regex:      `api=(\S+)`,
				LabelNames: []string{"api"},
				Metric:     "app_requests_total",
				Value:      1,
			},
			{
				Name:       "requests",
				Selector:   `{job="worker"}`,
				Regex:      `task=(\S+)`,
				LabelNames: []string{"task"},
				Metric:     "worker_tasks_total",
				Value:      1,
			},
		},
	}

	err := ins.Init()
	if err == nil {
		t.Fatal("expected duplicate rule name error")
	}
	if !strings.Contains(err.Error(), "duplicate rule name") {
		t.Fatalf("expected duplicate rule name error, got %v", err)
	}
}

func TestInstanceInitRejectsBackwardDirection(t *testing.T) {
	ins := &Instance{
		URL:       "http://loki.example.com",
		Direction: "backward",
		StateFile: filepath.Join(t.TempDir(), "state.json"),
		HTTPCommonConfig: config.HTTPCommonConfig{
			Method: "GET",
		},
		Rules: []*Rule{
			{
				Name:       "requests",
				Selector:   `{job="app"}`,
				Regex:      `api=(\S+)`,
				LabelNames: []string{"api"},
				Metric:     "app_requests_total",
				Value:      1,
			},
		},
	}

	err := ins.Init()
	if err == nil {
		t.Fatal("expected direction error")
	}
	if !strings.Contains(err.Error(), "direction must be forward") {
		t.Fatalf("expected direction error, got %v", err)
	}
}

func TestInstanceGatherDoesNotCommitCursorWhenLimitMayHaveTruncatedResults(t *testing.T) {
	now := time.Unix(1700000000, 0)
	ts1 := now.Add(-4 * time.Minute).UnixNano()
	stateFile := filepath.Join(t.TempDir(), "state.json")

	client := &rangeClient{
		limit: 2,
		entries: []logEntry{
			{Timestamp: ts1, Line: "api=/first", Stream: map[string]string{"job": "app"}},
			{Timestamp: ts1, Line: "api=/second", Stream: map[string]string{"job": "app"}},
			{Timestamp: ts1, Line: "api=/third", Stream: map[string]string{"job": "app"}},
		},
	}
	ins := newTestInstance(now, stateFile, 2)
	if err := ins.Init(); err != nil {
		t.Fatalf("init instance: %v", err)
	}
	ins.client = client

	ins.Gather(types.NewSampleList())

	gotState, err := loadState(stateFile)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if gotState.Rules["requests"].CursorTs != 0 {
		t.Fatalf("expected cursor to stay uncommitted when one timestamp still reaches limit, got %d", gotState.Rules["requests"].CursorTs)
	}
}

func TestInstanceGatherPaginatesWhenLimitReached(t *testing.T) {
	now := time.Unix(1700000000, 0)
	ts1 := now.Add(-4 * time.Minute).UnixNano()
	ts2 := now.Add(-3 * time.Minute).UnixNano()
	ts3 := now.Add(-2 * time.Minute).UnixNano()
	stateFile := filepath.Join(t.TempDir(), "state.json")

	client := &rangeClient{
		limit: 2,
		entries: []logEntry{
			{Timestamp: ts1, Line: "api=/first", Stream: map[string]string{"job": "app"}},
			{Timestamp: ts2, Line: "api=/second", Stream: map[string]string{"job": "app"}},
			{Timestamp: ts3, Line: "api=/third", Stream: map[string]string{"job": "app"}},
		},
	}
	ins := newTestInstance(now, stateFile, 2)
	if err := ins.Init(); err != nil {
		t.Fatalf("init instance: %v", err)
	}
	ins.client = client

	slist := types.NewSampleList()
	ins.Gather(slist)

	if len(client.requests) <= 1 {
		t.Fatalf("expected split range requests, got %d", len(client.requests))
	}
	expectedStart := now.Add(-5 * time.Minute).UnixNano()
	expectedEnd := now.UnixNano()
	if !hasRequestRange(t, client.requests, expectedStart, expectedEnd) {
		t.Fatalf("expected initial full range request [%d,%d]", expectedStart, expectedEnd)
	}
	if !hasNarrowerRequest(t, client.requests, expectedStart, expectedEnd) {
		t.Fatalf("expected at least one narrower split request inside [%d,%d]", expectedStart, expectedEnd)
	}
	gotState, err := loadState(stateFile)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if gotState.Rules["requests"].CursorTs != ts3 {
		t.Fatalf("expected cursor %d, got %d", ts3, gotState.Rules["requests"].CursorTs)
	}
	assertSampleValue(t, slist.PopBackAll(), "lokimtail_app_requests_total", map[string]string{"api": "/third"}, 1)
}

func newTestInstance(now time.Time, stateFile string, limit int) *Instance {
	return &Instance{
		URL:           "http://loki.example.com",
		Timeout:       config.Duration(2 * time.Second),
		IngestDelay:   0,
		Lookback:      config.Duration(5 * time.Minute),
		Overlap:       0,
		Limit:         limit,
		StateFile:     stateFile,
		MaxDedupItems: 128,
		nowFn:         func() time.Time { return now },
		HTTPCommonConfig: config.HTTPCommonConfig{
			Method: "GET",
		},
		Rules: []*Rule{
			{
				Name:       "requests",
				Selector:   `{job="app"}`,
				Regex:      `api=(\S+)`,
				LabelNames: []string{"api"},
				Metric:     "app_requests_total",
				Value:      1,
			},
		},
	}
}

func hasRequestRange(t *testing.T, requests []*http.Request, wantStart, wantEnd int64) bool {
	t.Helper()
	for _, req := range requests {
		start, end := requestRange(t, req)
		if start == wantStart && end == wantEnd {
			return true
		}
	}
	return false
}

func hasNarrowerRequest(t *testing.T, requests []*http.Request, fullStart, fullEnd int64) bool {
	t.Helper()
	for _, req := range requests {
		start, end := requestRange(t, req)
		if start >= fullStart && end <= fullEnd && (start != fullStart || end != fullEnd) {
			return true
		}
	}
	return false
}

func requestRange(t *testing.T, req *http.Request) (int64, int64) {
	t.Helper()
	start, err := strconv.ParseInt(req.URL.Query().Get("start"), 10, 64)
	if err != nil {
		t.Fatalf("parse start: %v", err)
	}
	end, err := strconv.ParseInt(req.URL.Query().Get("end"), 10, 64)
	if err != nil {
		t.Fatalf("parse end: %v", err)
	}
	return start, end
}

type fakeClient struct {
	body []byte
}

func (f fakeClient) Do(_ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(f.body)),
		Header:     make(http.Header),
	}, nil
}

type captureClient struct {
	body        []byte
	lastRequest *http.Request
}

func (f *captureClient) Do(req *http.Request) (*http.Response, error) {
	f.lastRequest = req
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(f.body)),
		Header:     make(http.Header),
	}, nil
}

type rangeClient struct {
	entries  []logEntry
	limit    int
	requests []*http.Request
}

func (f *rangeClient) Do(req *http.Request) (*http.Response, error) {
	f.requests = append(f.requests, req)
	query := req.URL.Query()
	start, err := strconv.ParseInt(query.Get("start"), 10, 64)
	if err != nil {
		return nil, err
	}
	end, err := strconv.ParseInt(query.Get("end"), 10, 64)
	if err != nil {
		return nil, err
	}
	var selected []logEntry
	for _, entry := range f.entries {
		if entry.Timestamp < start || entry.Timestamp > end {
			continue
		}
		selected = append(selected, entry)
		if len(selected) >= f.limit {
			break
		}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(lokiResponseBodyNoTest(selected))),
		Header:     make(http.Header),
	}, nil
}

func lokiResponseBody(t *testing.T, entries []logEntry) []byte {
	t.Helper()
	body, err := json.Marshal(lokiResponsePayload(entries))
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	return body
}

func lokiResponseBodyNoTest(entries []logEntry) []byte {
	body, _ := json.Marshal(lokiResponsePayload(entries))
	return body
}

func lokiResponsePayload(entries []logEntry) map[string]any {
	values := make([]any, 0, len(entries))
	stream := map[string]string{"job": "app"}
	if len(entries) > 0 && entries[0].Stream != nil {
		stream = entries[0].Stream
	}
	for _, entry := range entries {
		values = append(values, []string{int64ToString(entry.Timestamp), entry.Line})
	}
	resp := map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "streams",
			"result": []any{
				map[string]any{
					"stream": stream,
					"values": values,
				},
			},
		},
	}
	return resp
}

func assertSampleValue(t *testing.T, samples []*types.Sample, metric string, labels map[string]string, expected float64) {
	t.Helper()
	for _, sample := range samples {
		if sample.Metric != metric {
			continue
		}
		if !sameLabels(sample.Labels, labels) {
			continue
		}
		got, ok := sample.Value.(float64)
		if !ok {
			t.Fatalf("sample %s value is %T, expected float64", metric, sample.Value)
		}
		if got != expected {
			t.Fatalf("sample %s expected %v, got %v", metric, expected, got)
		}
		return
	}
	t.Fatalf("sample %s with labels %v not found", metric, labels)
}

func sameLabels(got, expected map[string]string) bool {
	if len(got) != len(expected) {
		return false
	}
	for k, v := range expected {
		if got[k] != v {
			return false
		}
	}
	return true
}
