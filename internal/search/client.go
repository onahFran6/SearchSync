package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8"
)

type Client struct {
	es    *elasticsearch.Client
	index string
}

func NewClient(es *elasticsearch.Client, index string) *Client {
	return &Client{es: es, index: index}
}

func (c *Client) Index() string { return c.index }

type searchResponse struct {
	Hits struct {
		Hits []struct {
			ID     string   `json:"_id"`
			Source Document `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

func (c *Client) Search(ctx context.Context, q Query) ([]Document, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(buildRequestBody(q)); err != nil {
		return nil, fmt.Errorf("encode search request: %w", err)
	}

	res, err := c.es.Search(
		c.es.Search.WithContext(ctx),
		c.es.Search.WithIndex(c.index),
		c.es.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, fmt.Errorf("execute search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch search error: %s", res.String())
	}

	var parsed searchResponse
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode search response: %w", err)
	}

	docs := make([]Document, 0, len(parsed.Hits.Hits))
	for _, h := range parsed.Hits.Hits {
		doc := h.Source
		doc.ID = h.ID // the ES document _id is the authoritative product id, not a mapped field
		docs = append(docs, doc)
	}
	return docs, nil
}

// IndexDocument upserts doc at the given id, using the source primary key as
// the stable Elasticsearch document id so repeated create/update/snapshot
// events for the same product converge rather than accumulate duplicates.
func (c *Client) IndexDocument(ctx context.Context, id string, doc Document) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(doc); err != nil {
		return fmt.Errorf("encode document %s: %w", id, err)
	}

	res, err := c.es.Index(
		c.index,
		&buf,
		c.es.Index.WithDocumentID(id),
		c.es.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("index document %s: %w", id, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch index error for %s: %s", id, res.String())
	}
	return nil
}

// DeleteDocument removes id from the index. A 404 (already absent) is
// treated as success so at-least-once redelivery of a delete event, or a
// delete racing an earlier delete, is idempotent rather than an error.
func (c *Client) DeleteDocument(ctx context.Context, id string) error {
	res, err := c.es.Delete(
		c.index,
		id,
		c.es.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("delete document %s: %w", id, err)
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("elasticsearch delete error for %s: %s", id, res.String())
	}
	return nil
}
