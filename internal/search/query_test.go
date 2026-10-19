package search

import "testing"

func TestBuildRequestBody(t *testing.T) {
	min := 1000
	max := 5000

	tests := []struct {
		name            string
		q               Query
		wantMatchAll    bool
		wantFilters     int
		wantSizeAtLeast int
	}{
		{name: "empty text uses match_all", q: Query{}, wantMatchAll: true, wantSizeAtLeast: 1},
		{name: "text query", q: Query{Text: "macbook"}, wantMatchAll: false, wantSizeAtLeast: 1},
		{name: "category filter only", q: Query{Category: "accessories"}, wantMatchAll: true, wantFilters: 1},
		{
			name:         "price range filter",
			q:            Query{MinPriceCents: &min, MaxPriceCents: &max},
			wantMatchAll: true,
			wantFilters:  1,
		},
		{
			name:         "category and price filters combined",
			q:            Query{Category: "accessories", MinPriceCents: &min},
			wantMatchAll: true,
			wantFilters:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := buildRequestBody(tt.q)

			size, ok := body["size"].(int)
			if !ok || size < tt.wantSizeAtLeast {
				t.Fatalf("expected size >= %d, got %v", tt.wantSizeAtLeast, body["size"])
			}

			query := body["query"].(map[string]any)["bool"].(map[string]any)
			must := query["must"].([]map[string]any)
			if len(must) != 1 {
				t.Fatalf("expected exactly one must clause, got %d", len(must))
			}
			_, isMatchAll := must[0]["match_all"]
			if isMatchAll != tt.wantMatchAll {
				t.Fatalf("match_all = %v, want %v", isMatchAll, tt.wantMatchAll)
			}

			filter := query["filter"].([]map[string]any)
			if len(filter) != tt.wantFilters {
				t.Fatalf("got %d filters, want %d", len(filter), tt.wantFilters)
			}
		})
	}
}

func TestBuildRequestBody_DefaultSize(t *testing.T) {
	body := buildRequestBody(Query{})
	if body["size"] != 20 {
		t.Fatalf("expected default size 20, got %v", body["size"])
	}
}
