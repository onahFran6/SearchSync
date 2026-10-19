package products

import "testing"

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }

func TestValidateCreate(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateRequest
		wantErr bool
	}{
		{
			name: "valid",
			req:  CreateRequest{SKU: "A", Name: "Widget", Category: "c", PriceCents: 100},
		},
		{
			name:    "missing sku",
			req:     CreateRequest{Name: "Widget", Category: "c", PriceCents: 100},
			wantErr: true,
		},
		{
			name:    "missing name",
			req:     CreateRequest{SKU: "A", Category: "c", PriceCents: 100},
			wantErr: true,
		},
		{
			name:    "missing category",
			req:     CreateRequest{SKU: "A", Name: "Widget", PriceCents: 100},
			wantErr: true,
		},
		{
			name:    "negative price",
			req:     CreateRequest{SKU: "A", Name: "Widget", Category: "c", PriceCents: -1},
			wantErr: true,
		},
		{
			name:    "whitespace-only name",
			req:     CreateRequest{SKU: "A", Name: "   ", Category: "c", PriceCents: 100},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCreate(tt.req)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateUpdate(t *testing.T) {
	tests := []struct {
		name    string
		req     UpdateRequest
		wantErr bool
	}{
		{name: "no fields supplied", req: UpdateRequest{}},
		{name: "valid price update", req: UpdateRequest{PriceCents: intPtr(500)}},
		{name: "empty name", req: UpdateRequest{Name: strPtr("")}, wantErr: true},
		{name: "empty category", req: UpdateRequest{Category: strPtr("  ")}, wantErr: true},
		{name: "negative price", req: UpdateRequest{PriceCents: intPtr(-5)}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUpdate(tt.req)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
