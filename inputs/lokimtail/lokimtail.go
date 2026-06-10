package lokimtail

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"flashcat.cloud/categraf/config"
	"flashcat.cloud/categraf/inputs"
	"flashcat.cloud/categraf/pkg/httpx"
	"flashcat.cloud/categraf/types"
)

const inputName = "lokimtail"

const defaultMaxQueryRange = 720 * time.Hour

type LokiMTail struct {
	config.PluginConfig
	Instances []*Instance `toml:"instances"`
}

type Rule struct {
	Name         string            `toml:"name"`
	Selector     string            `toml:"selector"`
	Include      []string          `toml:"include"`
	Exclude      []string          `toml:"exclude"`
	Regex        string            `toml:"regex"`
	LabelNames   []string          `toml:"label_names"`
	MetricLabels []string          `toml:"metric_labels"`
	Metric       string            `toml:"metric"`
	ValueFrom    string            `toml:"value_from"`
	Value        float64           `toml:"value"`
	Labels       map[string]string `toml:"labels"`

	regex *regexp.Regexp `toml:"-"`
}

type Instance struct {
	config.InstanceConfig

	URL           string            `toml:"url"`
	Timeout       config.Duration   `toml:"timeout"`
	IngestDelay   config.Duration   `toml:"ingest_delay"`
	Overlap       config.Duration   `toml:"overlap"`
	Lookback      config.Duration   `toml:"lookback"`
	MaxQueryRange config.Duration   `toml:"max_query_range"`
	Limit         int               `toml:"limit"`
	Direction     string            `toml:"direction"`
	StateFile     string            `toml:"state_file"`
	MaxDedupItems int               `toml:"max_dedup_items"`
	StaticLabels  map[string]string `toml:"static_labels"`
	Rules         []*Rule           `toml:"rules"`

	config.HTTPCommonConfig

	client httpClient   `toml:"-"`
	state  *pluginState `toml:"-"`
	mu     sync.Mutex   `toml:"-"`
	nowFn  func() time.Time
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type lokiQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string       `json:"resultType"`
		Result     []lokiStream `json:"result"`
	} `json:"data"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

type logEntry struct {
	Timestamp int64
	Line      string
	Stream    map[string]string
}

func init() {
	inputs.Add(inputName, func() inputs.Input {
		return &LokiMTail{}
	})
}

func (l *LokiMTail) Clone() inputs.Input {
	return &LokiMTail{}
}

func (l *LokiMTail) Name() string {
	return inputName
}

func (l *LokiMTail) GetInstances() []inputs.Instance {
	ret := make([]inputs.Instance, len(l.Instances))
	for i := 0; i < len(l.Instances); i++ {
		ret[i] = l.Instances[i]
	}
	return ret
}

func (ins *Instance) Init() error {
	if len(ins.Rules) == 0 {
		return types.ErrInstancesEmpty
	}
	if ins.URL == "" {
		return errors.New("url is required")
	}
	if _, err := url.Parse(ins.URL); err != nil {
		return fmt.Errorf("invalid loki url: %w", err)
	}
	if ins.Timeout <= 0 {
		ins.Timeout = config.Duration(3 * time.Second)
	}
	if ins.IngestDelay < 0 {
		return errors.New("ingest_delay must be >= 0")
	}
	if ins.Overlap < 0 {
		return errors.New("overlap must be >= 0")
	}
	if ins.Lookback <= 0 {
		ins.Lookback = config.Duration(5 * time.Minute)
	}
	if ins.MaxQueryRange <= 0 {
		ins.MaxQueryRange = config.Duration(defaultMaxQueryRange)
	}
	if ins.Limit <= 0 {
		ins.Limit = 1000
	}
	if ins.Direction == "" {
		ins.Direction = "forward"
	}
	if ins.Direction != "forward" {
		return errors.New("direction must be forward")
	}
	if ins.MaxDedupItems <= 0 {
		ins.MaxDedupItems = 50000
	}
	if ins.nowFn == nil {
		ins.nowFn = time.Now
	}
	ins.InitHTTPClientConfig()
	ins.Timeout = maxDuration(ins.Timeout, config.Duration(3*time.Second))

	ruleNames := make(map[string]struct{}, len(ins.Rules))
	for _, rule := range ins.Rules {
		if _, ok := ruleNames[rule.Name]; ok && rule.Name != "" {
			return fmt.Errorf("duplicate rule name %q", rule.Name)
		}
		ruleNames[rule.Name] = struct{}{}
		if err := rule.init(); err != nil {
			return err
		}
	}

	state, err := loadState(ins.StateFile)
	if err != nil {
		return err
	}
	ins.state = state

	client, err := ins.createHTTPClient()
	if err != nil {
		return err
	}
	ins.client = client
	return nil
}

func (r *Rule) init() error {
	if r.Name == "" {
		return errors.New("rule name is required")
	}
	if r.Selector == "" {
		return fmt.Errorf("rule %s selector is required", r.Name)
	}
	if r.Metric == "" {
		return fmt.Errorf("rule %s metric is required", r.Name)
	}
	if r.Regex == "" {
		return fmt.Errorf("rule %s regex is required", r.Name)
	}
	reg, err := regexp.Compile(r.Regex)
	if err != nil {
		return fmt.Errorf("rule %s regex compile failed: %w", r.Name, err)
	}
	r.regex = reg
	if reg.NumSubexp() != len(r.LabelNames) {
		return fmt.Errorf("rule %s label_names length %d does not match capture groups %d", r.Name, len(r.LabelNames), reg.NumSubexp())
	}
	if r.ValueFrom == "" && r.Value == 0 {
		r.Value = 1
	}
	if r.ValueFrom != "" && !contains(r.LabelNames, r.ValueFrom) {
		return fmt.Errorf("rule %s value_from %q not found in label_names", r.Name, r.ValueFrom)
	}
	for _, name := range r.MetricLabels {
		if !contains(r.LabelNames, name) {
			return fmt.Errorf("rule %s metric_labels %q not found in label_names", r.Name, name)
		}
	}
	if r.Labels == nil {
		r.Labels = map[string]string{}
	}
	return nil
}

func (ins *Instance) createHTTPClient() (*http.Client, error) {
	tlsCfg, err := ins.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}
	client := httpx.CreateHTTPClient(
		httpx.TlsConfig(tlsCfg),
		httpx.Timeout(time.Duration(ins.Timeout)),
		httpx.DisableKeepAlives(true),
		httpx.FollowRedirects(false),
	)
	return client, nil
}

func (ins *Instance) Gather(slist *types.SampleList) {
	ins.mu.Lock()
	defer ins.mu.Unlock()

	if ins.state == nil {
		return
	}

	now := ins.nowFn()
	queryEnd := now.Add(-time.Duration(ins.IngestDelay))
	for _, rule := range ins.Rules {
		ruleState := ins.state.ensureRule(rule.Name)
		startNs := ins.initialStart(ruleState, queryEnd)
		endNs := queryEnd.UnixNano()
		if maxEndNs := time.Unix(0, startNs).Add(time.Duration(ins.MaxQueryRange)).UnixNano(); maxEndNs < endNs {
			endNs = maxEndNs
		}
		if startNs > endNs {
			continue
		}

		entries, complete, err := ins.queryRangeAll(rule, startNs, endNs)
		if err != nil {
			log.Println("E! lokimtail query failed:", rule.Name, err)
			continue
		}

		var maxTs int64
		for _, entry := range entries {
			if ins.state.seen(rule.Name, eventFingerprint(entry)) {
				continue
			}

			updated, err := rule.apply(entry.Line, mergeLabels(ins.GetLabels(), ins.StaticLabels))
			if err != nil {
				log.Println("E! lokimtail apply rule failed:", rule.Name, err)
				continue
			}
			for _, sample := range updated {
				seriesKey := buildSeriesKey(sample.Metric, sample.Labels)
				ruleState.Counters[seriesKey] += sample.Value.(float64)
			}
			ins.state.remember(rule.Name, eventFingerprint(entry), entry.Timestamp, ins.MaxDedupItems)
			if entry.Timestamp > maxTs {
				maxTs = entry.Timestamp
			}
		}

		for seriesKey, value := range ruleState.Counters {
			metric, labels := parseSeriesKey(seriesKey)
			slist.PushFront(types.NewSample("", metric, value, labels))
		}

		if !complete {
			log.Printf("W! lokimtail query may be truncated for rule %s, cursor not advanced to avoid skipping logs", rule.Name)
			continue
		}
		if maxTs > 0 {
			ruleState.CursorTs = maxTs
		} else if len(entries) == 0 && endNs > ruleState.CursorTs {
			ruleState.CursorTs = endNs
		}
	}

	if err := saveState(ins.StateFile, ins.state); err != nil {
		log.Println("E! lokimtail save state failed:", err)
	}
}

func (ins *Instance) initialStart(rs *ruleState, queryEnd time.Time) int64 {
	if rs.CursorTs > 0 {
		start := rs.CursorTs - int64(ins.Overlap)
		if start < 0 {
			return 0
		}
		return start
	}
	start := queryEnd.Add(-time.Duration(ins.Lookback)).UnixNano()
	if start < 0 {
		return 0
	}
	return start
}

func (ins *Instance) queryRangeAll(rule *Rule, startNs, endNs int64) ([]logEntry, bool, error) {
	return ins.queryRangeSplit(rule, startNs, endNs)
}

func (ins *Instance) queryRangeSplit(rule *Rule, startNs, endNs int64) ([]logEntry, bool, error) {
	entries, err := ins.queryRange(rule, startNs, endNs)
	if err != nil {
		return nil, false, err
	}
	if len(entries) < ins.Limit {
		return entries, true, nil
	}
	if startNs >= endNs {
		return entries, false, nil
	}

	midNs := startNs + (endNs-startNs)/2
	left, leftComplete, err := ins.queryRangeSplit(rule, startNs, midNs)
	if err != nil {
		return nil, false, err
	}
	right, rightComplete, err := ins.queryRangeSplit(rule, midNs+1, endNs)
	if err != nil {
		return nil, false, err
	}
	return append(left, right...), leftComplete && rightComplete, nil
}

func (ins *Instance) queryRange(rule *Rule, startNs, endNs int64) ([]logEntry, error) {
	parsed, err := url.Parse(ins.URL)
	if err != nil {
		return nil, err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/loki/api/v1/query_range"

	query := parsed.Query()
	query.Set("query", buildQuery(rule))
	query.Set("start", strconv.FormatInt(startNs, 10))
	query.Set("end", strconv.FormatInt(endNs, 10))
	query.Set("limit", strconv.Itoa(ins.Limit))
	query.Set("direction", ins.Direction)
	parsed.RawQuery = query.Encode()

	req, err := http.NewRequest(ins.Method, parsed.String(), ins.GetBody())
	if err != nil {
		return nil, err
	}
	ins.SetHeaders(req)

	resp, err := ins.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("unexpected loki status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload lokiQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	entries := flattenStreams(payload.Data.Result)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Timestamp == entries[j].Timestamp {
			return canonicalStream(entries[i].Stream) < canonicalStream(entries[j].Stream)
		}
		return entries[i].Timestamp < entries[j].Timestamp
	})
	return entries, nil
}

func (r *Rule) apply(line string, baseLabels map[string]string) ([]*types.Sample, error) {
	matches := r.regex.FindStringSubmatch(line)
	if len(matches) == 0 {
		return nil, nil
	}

	labels := mergeLabels(baseLabels, r.Labels)
	value := r.Value
	for idx, name := range r.LabelNames {
		captured := matches[idx+1]
		if len(r.MetricLabels) == 0 || contains(r.MetricLabels, name) {
			labels[name] = captured
		}
		if name == r.ValueFrom {
			parsed, err := strconv.ParseFloat(captured, 64)
			if err != nil {
				return nil, fmt.Errorf("parse value_from %s: %w", r.ValueFrom, err)
			}
			value = parsed
		}
	}

	return []*types.Sample{
		types.NewSample(inputName, r.Metric, value, labels),
	}, nil
}

func buildQuery(rule *Rule) string {
	var b strings.Builder
	b.WriteString(rule.Selector)
	for _, item := range rule.Include {
		if item == "" {
			continue
		}
		b.WriteString("|~")
		b.WriteString(strconv.Quote(item))
	}
	for _, item := range rule.Exclude {
		if item == "" {
			continue
		}
		b.WriteString("!~")
		b.WriteString(strconv.Quote(item))
	}
	return b.String()
}

func flattenStreams(streams []lokiStream) []logEntry {
	entries := make([]logEntry, 0)
	for _, stream := range streams {
		for _, pair := range stream.Values {
			if len(pair) < 2 {
				continue
			}
			ts, err := strconv.ParseInt(pair[0], 10, 64)
			if err != nil {
				continue
			}
			entries = append(entries, logEntry{
				Timestamp: ts,
				Line:      pair[1],
				Stream:    stream.Stream,
			})
		}
	}
	return entries
}

func canonicalStream(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, ",")
}

func eventFingerprint(entry logEntry) string {
	sum := sha1.Sum([]byte(canonicalStream(entry.Stream) + "\n" + strconv.FormatInt(entry.Timestamp, 10) + "\n" + entry.Line))
	return hex.EncodeToString(sum[:])
}

func mergeLabels(groups ...map[string]string) map[string]string {
	out := make(map[string]string)
	for _, group := range groups {
		for key, value := range group {
			out[key] = value
		}
	}
	return out
}

func buildSeriesKey(metric string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys)+1)
	parts = append(parts, metric)
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, "|")
}

func parseSeriesKey(series string) (string, map[string]string) {
	parts := strings.Split(series, "|")
	labels := make(map[string]string, max(0, len(parts)-1))
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		labels[kv[0]] = kv[1]
	}
	return parts[0], labels
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func maxDuration(a, b config.Duration) config.Duration {
	if a > b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func int64ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}

type persistedSample struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

type pluginState struct {
	Version int                   `json:"version"`
	Rules   map[string]*ruleState `json:"rules"`
}

type ruleState struct {
	CursorTs int64              `json:"cursor_ts"`
	Counters map[string]float64 `json:"counters"`
	Recent   map[string]int64   `json:"recent"`
}

func loadState(path string) (*pluginState, error) {
	st := &pluginState{
		Version: 1,
		Rules:   map[string]*ruleState{},
	}
	if path == "" {
		return st, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return st, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return st, nil
	}
	if err := json.Unmarshal(data, st); err != nil {
		return nil, err
	}
	if st.Rules == nil {
		st.Rules = map[string]*ruleState{}
	}
	return st, nil
}

func saveState(path string, st *pluginState) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (st *pluginState) ensureRule(name string) *ruleState {
	if st.Rules == nil {
		st.Rules = map[string]*ruleState{}
	}
	if _, ok := st.Rules[name]; !ok {
		st.Rules[name] = &ruleState{
			Counters: map[string]float64{},
			Recent:   map[string]int64{},
		}
	}
	if st.Rules[name].Counters == nil {
		st.Rules[name].Counters = map[string]float64{}
	}
	if st.Rules[name].Recent == nil {
		st.Rules[name].Recent = map[string]int64{}
	}
	return st.Rules[name]
}

func (st *pluginState) seen(ruleName, fingerprint string) bool {
	rs := st.ensureRule(ruleName)
	_, ok := rs.Recent[fingerprint]
	return ok
}

func (st *pluginState) remember(ruleName, fingerprint string, ts int64, maxItems int) {
	rs := st.ensureRule(ruleName)
	rs.Recent[fingerprint] = ts
	if len(rs.Recent) <= maxItems {
		return
	}

	type item struct {
		key string
		ts  int64
	}
	all := make([]item, 0, len(rs.Recent))
	for key, val := range rs.Recent {
		all = append(all, item{key: key, ts: val})
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].ts < all[j].ts
	})
	for len(all) > maxItems {
		delete(rs.Recent, all[0].key)
		all = all[1:]
	}
}
