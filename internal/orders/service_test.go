package orders

import "testing"

func TestValidateCreate(t *testing.T) {
	validCustomerID := "4a6c6e2a-1e4b-4f0a-9c3a-7b2a2e9c1234"

	tests := []struct {
		name           string
		req            CreateRequest
		idempotencyKey string
		wantErr        bool
	}{
		{
			name:           "valid",
			req:            CreateRequest{CustomerID: validCustomerID, TotalCents: 100},
			idempotencyKey: "key-1",
		},
		{
			name:           "missing idempotency key",
			req:            CreateRequest{CustomerID: validCustomerID, TotalCents: 100},
			idempotencyKey: "",
			wantErr:        true,
		},
		{
			name:           "whitespace-only idempotency key",
			req:            CreateRequest{CustomerID: validCustomerID, TotalCents: 100},
			idempotencyKey: "   ",
			wantErr:        true,
		},
		{
			name:           "invalid customer id",
			req:            CreateRequest{CustomerID: "not-a-uuid", TotalCents: 100},
			idempotencyKey: "key-1",
			wantErr:        true,
		},
		{
			name:           "negative total",
			req:            CreateRequest{CustomerID: validCustomerID, TotalCents: -1},
			idempotencyKey: "key-1",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCreate(tt.req, tt.idempotencyKey)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
