package search

// Document is the Elasticsearch read model, defined separately from
// products.Product so the search index shape can evolve independently of
// the authoritative PostgreSQL schema.
type Document struct {
	ID          string `json:"id"`
	SKU         string `json:"sku"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	PriceCents  int    `json:"price_cents"`
}
