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

// StatusError carries an Elasticsearch HTTP response's status code so
// callers can classify failures as transient (5xx, 429) versus permanent
// (other 4xx, e.g. a mapping rejection) without string-matching the
// response body.
type StatusError struct {
	Op   string
	ID   string
	Code int
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("elasticsearch %s error for %s: status %d: %s", e.Op, e.ID, e.Code, e.Body)
}

// Temporary reports whether the same request is expected to succeed on a
// later attempt: a server-side error or backpressure, not a request the
// cluster has permanently rejected.
func (e *StatusError) Temporary() bool {
	return e.Code >= 500 || e.Code == http.StatusTooManyRequests
}

const versionConflictStatus = http.StatusConflict

// GetDocument fetches a single document by id. It returns found=false (not
// an error) for a 404, mirroring DeleteDocument's treatment of absence as a
// normal outcome rather than a failure. Intended for diagnostics and tests
// that need to assert on current index state directly, not for the
// consumer's own hot path.
func (c *Client) GetDocument(ctx context.Context, id string) (doc Document, found bool, err error) {
	res, err := c.es.Get(c.index, id, c.es.Get.WithContext(ctx))
	if err != nil {
		return Document{}, false, fmt.Errorf("get document %s: %w", id, err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return Document{}, false, nil
	}
	if res.IsError() {
		return Document{}, false, &StatusError{Op: "get", ID: id, Code: res.StatusCode, Body: res.String()}
	}

	var parsed struct {
		Source Document `json:"_source"`
	}
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return Document{}, false, fmt.Errorf("decode get response for %s: %w", id, err)
	}
	parsed.Source.ID = id
	return parsed.Source, true, nil
}

// IndexDocument upserts doc at the given id, using the source primary key as
// the stable Elasticsearch document id so repeated create/update/snapshot
// events for the same product converge rather than accumulate duplicates.
//
// version is the Debezium source LSN for the change being applied, sent as
// an external version so Elasticsearch itself rejects the write if a
// greater-or-equal version is already stored. That rejection (409) is not
// an error here: it means a newer or equal state already won, which is
// exactly the outcome stale-replay protection exists to guarantee, so it
// is swallowed as a no-op rather than retried or escalated.
func (c *Client) IndexDocument(ctx context.Context, id string, doc Document, version int64) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(doc); err != nil {
		return fmt.Errorf("encode document %s: %w", id, err)
	}

	res, err := c.es.Index(
		c.index,
		&buf,
		c.es.Index.WithDocumentID(id),
		c.es.Index.WithContext(ctx),
		c.es.Index.WithVersion(int(version)),
		c.es.Index.WithVersionType("external"),
	)
	if err != nil {
		return fmt.Errorf("index document %s: %w", id, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == versionConflictStatus {
			return nil
		}
		return &StatusError{Op: "index", ID: id, Code: res.StatusCode, Body: res.String()}
	}
	return nil
}

// DeleteDocument removes id from the index. A 404 (already absent) is
// treated as success so at-least-once redelivery of a delete event, or a
// delete racing an earlier delete, is idempotent rather than an error. A
// 409 version conflict is likewise a no-op for the same stale-replay
// reason as IndexDocument: a later state already won.
func (c *Client) DeleteDocument(ctx context.Context, id string, version int64) error {
	res, err := c.es.Delete(
		c.index,
		id,
		c.es.Delete.WithContext(ctx),
		c.es.Delete.WithVersion(int(version)),
		c.es.Delete.WithVersionType("external"),
	)
	if err != nil {
		return fmt.Errorf("delete document %s: %w", id, err)
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != http.StatusNotFound {
		if res.StatusCode == versionConflictStatus {
			return nil
		}
		return &StatusError{Op: "delete", ID: id, Code: res.StatusCode, Body: res.String()}
	}
	return nil
}
