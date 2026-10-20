package indexer

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/francisonah/searchsync/internal/platform"
	"github.com/francisonah/searchsync/internal/search"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want platform.FailureKind
	}{
		{"malformed envelope", ErrPermanent, platform.FailurePermanent},
		{"es 503", &search.StatusError{Code: 503}, platform.FailureTransient},
		{"es 429", &search.StatusError{Code: 429}, platform.FailureTransient},
		{"es 400 mapping rejection", &search.StatusError{Code: 400}, platform.FailurePermanent},
		{"network error", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, platform.FailureTransient},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classify(tt.err); got != tt.want {
				t.Fatalf("classify(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestHandleMessage_MalformedEnvelopeClassifiesPermanent(t *testing.T) {
	h := NewHandler(nil)
	err := h.HandleMessage(context.Background(), []byte("not json"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
	if classify(err) != platform.FailurePermanent {
		t.Fatalf("expected permanent classification")
	}
}

func TestHandleMessage_UnsupportedOpClassifiesPermanent(t *testing.T) {
	h := NewHandler(nil)
	err := h.HandleMessage(context.Background(), []byte(`{"op":"t"}`))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}
