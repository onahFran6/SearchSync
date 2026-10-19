package platform

import (
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
)

func NewElasticsearchClient(addr string) (*elasticsearch.Client, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{addr},
	})
	if err != nil {
		return nil, fmt.Errorf("create elasticsearch client: %w", err)
	}
	return client, nil
}
