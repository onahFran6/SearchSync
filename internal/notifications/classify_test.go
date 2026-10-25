package notifications

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/onahFran6/SearchSync/internal/platform"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want platform.FailureKind
	}{
		{"malformed event", ErrPermanent, platform.FailurePermanent},
		{"not null violation", &pgconn.PgError{Code: "23502"}, platform.FailurePermanent},
		{"check violation", &pgconn.PgError{Code: "23514"}, platform.FailurePermanent},
		{"invalid text representation", &pgconn.PgError{Code: "22P02"}, platform.FailurePermanent},
		{"foreign key violation", &pgconn.PgError{Code: "23503"}, platform.FailurePermanent},
		{"connection exception", &pgconn.PgError{Code: "08006"}, platform.FailureTransient},
		{"cannot connect now", &pgconn.PgError{Code: "57P03"}, platform.FailureTransient},
		{"serialization failure", &pgconn.PgError{Code: "40001"}, platform.FailureTransient},
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

func TestHandleMessage_MalformedPayloadClassifiesPermanent(t *testing.T) {
	h := NewHandler(nil)
	err := h.HandleMessage(context.Background(), []byte("not json"))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}

func TestHandleMessage_MissingEventIDClassifiesPermanent(t *testing.T) {
	h := NewHandler(nil)
	err := h.HandleMessage(context.Background(), []byte(`{"orderId":"x"}`))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}
