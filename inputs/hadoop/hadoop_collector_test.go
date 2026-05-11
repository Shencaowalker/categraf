package hadoop

import "testing"

func TestParseJMXDataAcceptsNaN(t *testing.T) {
	data, err := parseJMXData([]byte(`{
		"beans": [{
			"name": "Hadoop:service=NodeManager,name=NodeManagerMetrics",
			"ContainersLaunched": 3,
			"blockTransferAvgSize_1min": NaN
		}]
	}`))
	if err != nil {
		t.Fatalf("parseJMXData returned error: %v", err)
	}

	beans, ok := data["beans"].([]interface{})
	if !ok || len(beans) != 1 {
		t.Fatalf("expected one bean, got %#v", data["beans"])
	}

	bean, ok := beans[0].(map[string]interface{})
	if !ok {
		t.Fatalf("expected bean map, got %#v", beans[0])
	}

	if got := bean["ContainersLaunched"]; got != float64(3) {
		t.Fatalf("expected ContainersLaunched to survive NaN cleanup, got %#v", got)
	}

	if got, exists := bean["blockTransferAvgSize_1min"]; !exists || got != nil {
		t.Fatalf("expected NaN to be decoded as nil, got %#v", got)
	}
}
