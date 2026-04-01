package lokimtail

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
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
