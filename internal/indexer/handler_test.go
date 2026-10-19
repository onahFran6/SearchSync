package indexer

import (
	"context"
	"testing"
)

func TestHandleMessage_TombstoneIsNoOp(t *testing.T) {
	h := NewHandler(nil) // no Elasticsearch call should happen for a nil value
	if err := h.HandleMessage(context.Background(), nil); err != nil {
		t.Fatalf("expected nil error for tombstone, got %v", err)
	}
}

func TestHandleMessage_UnsupportedOp(t *testing.T) {
	h := NewHandler(nil)
	err := h.HandleMessage(context.Background(), []byte(`{"op":"t"}`))
	if err == nil {
		t.Fatal("expected error for unsupported op")
	}
}

func TestHandleMessage_MalformedEnvelope(t *testing.T) {
	h := NewHandler(nil)
	err := h.HandleMessage(context.Background(), []byte(`not json`))
	if err == nil {
		t.Fatal("expected error for malformed envelope")
	}
}
