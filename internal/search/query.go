package search

type Query struct {
	Text          string
	Category      string
	MinPriceCents *int
	MaxPriceCents *int
	Size          int
}

func buildRequestBody(q Query) map[string]any {
	var must []map[string]any
	if q.Text != "" {
		must = append(must, map[string]any{
			"multi_match": map[string]any{
				"query":  q.Text,
				"fields": []string{"name^2", "description"},
			},
		})
	} else {
		must = append(must, map[string]any{"match_all": map[string]any{}})
	}

	var filter []map[string]any
	if q.Category != "" {
		filter = append(filter, map[string]any{"term": map[string]any{"category": q.Category}})
	}
	if q.MinPriceCents != nil || q.MaxPriceCents != nil {
		priceRange := map[string]any{}
		if q.MinPriceCents != nil {
			priceRange["gte"] = *q.MinPriceCents
		}
		if q.MaxPriceCents != nil {
			priceRange["lte"] = *q.MaxPriceCents
		}
		filter = append(filter, map[string]any{"range": map[string]any{"price_cents": priceRange}})
	}

	size := q.Size
	if size <= 0 {
		size = 20
	}

	return map[string]any{
		"size": size,
		"query": map[string]any{
			"bool": map[string]any{
				"must":   must,
				"filter": filter,
			},
		},
	}
}
