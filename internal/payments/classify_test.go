package payments

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/francisonah/searchsync/internal/platform"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want platform.FailureKind
	}{
		{"malformed command", ErrPermanent, platform.FailurePermanent},
		{"not null violation", &pgconn.PgError{Code: "23502"}, platform.FailurePermanent},
		{"check violation", &pgconn.PgError{Code: "23514"}, platform.FailurePermanent},
		{"invalid text representation", &pgconn.PgError{Code: "22P02"}, platform.FailurePermanent},
		{"foreign key violation", &pgconn.PgError{Code: "23503"}, platform.FailurePermanent},
		{"connection exception", &pgconn.PgError{Code: "08006"}, platform.FailureTransient},
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

func TestHandleAuthorizePayment_MalformedPayloadClassifiesPermanent(t *testing.T) {
	s := NewService(nil, NewRepository())
	err := s.HandleAuthorizePayment(context.Background(), []byte("not json"))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}

func TestHandleAuthorizePayment_MissingFieldsClassifiesPermanent(t *testing.T) {
	s := NewService(nil, NewRepository())
	err := s.HandleAuthorizePayment(context.Background(), []byte(`{"orderId":"x"}`))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}
