package saga

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
		{"malformed message", ErrPermanent, platform.FailurePermanent},
		{"not null violation", &pgconn.PgError{Code: "23502"}, platform.FailurePermanent},
		{"check violation", &pgconn.PgError{Code: "23514"}, platform.FailurePermanent},
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

func newTestService() *Service {
	return NewService(nil, NewRepository(), DefaultConfig, nil)
}

func TestHandleOrderCreated_MalformedPayloadClassifiesPermanent(t *testing.T) {
	s := newTestService()
	err := s.HandleOrderCreated(context.Background(), []byte("not json"))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}

func TestHandleOrderCreated_MissingFieldsClassifiesPermanent(t *testing.T) {
	s := newTestService()
	err := s.HandleOrderCreated(context.Background(), []byte(`{"orderId":"x"}`))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}

func TestHandleInventoryReserved_MalformedPayloadClassifiesPermanent(t *testing.T) {
	s := newTestService()
	err := s.HandleInventoryReserved(context.Background(), []byte("not json"))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}

func TestHandlePaymentAuthorized_MalformedPayloadClassifiesPermanent(t *testing.T) {
	s := newTestService()
	err := s.HandlePaymentAuthorized(context.Background(), []byte("not json"))
	if !errors.Is(err, ErrPermanent) {
		t.Fatalf("expected ErrPermanent, got %v", err)
	}
}
