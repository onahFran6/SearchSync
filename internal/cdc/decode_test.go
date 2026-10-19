package cdc

import "testing"

func TestEnvelope_Row(t *testing.T) {
	tests := []struct {
		name    string
		env     Envelope
		want    ProductRow
		wantErr bool
	}{
		{
			name: "create",
			env: Envelope{
				Op:    OpCreate,
				After: []byte(`{"id":1,"sku":"A","name":"Widget","description":"d","category":"c","price_cents":100}`),
			},
			want: ProductRow{ID: 1, SKU: "A", Name: "Widget", Description: "d", Category: "c", PriceCents: 100},
		},
		{
			name: "update",
			env: Envelope{
				Op:    OpUpdate,
				After: []byte(`{"id":2,"sku":"B","name":"Gadget","category":"c","price_cents":200}`),
			},
			want: ProductRow{ID: 2, SKU: "B", Name: "Gadget", Category: "c", PriceCents: 200},
		},
		{
			name: "snapshot read",
			env: Envelope{
				Op:    OpSnapshotRead,
				After: []byte(`{"id":3,"sku":"C","name":"Thing","category":"c","price_cents":300}`),
			},
			want: ProductRow{ID: 3, SKU: "C", Name: "Thing", Category: "c", PriceCents: 300},
		},
		{
			name:    "delete has no row to index",
			env:     Envelope{Op: OpDelete, Before: []byte(`{"id":4}`)},
			wantErr: true,
		},
		{
			name:    "malformed after payload",
			env:     Envelope{Op: OpCreate, After: []byte(`not json`)},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.env.Row()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestEnvelope_DeletedID(t *testing.T) {
	tests := []struct {
		name    string
		before  []byte
		want    int64
		wantErr bool
	}{
		{name: "valid id", before: []byte(`{"id":42}`), want: 42},
		{name: "malformed before payload", before: []byte(`not json`), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := Envelope{Op: OpDelete, Before: tt.before}
			got, err := env.DeletedID()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDecodeEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		raw     []byte
		wantOp  string
		wantErr bool
	}{
		{name: "create envelope", raw: []byte(`{"op":"c","after":{"id":1}}`), wantOp: OpCreate},
		{name: "malformed json", raw: []byte(`{`), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := DecodeEnvelope(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if env.Op != tt.wantOp {
				t.Fatalf("got op %q, want %q", env.Op, tt.wantOp)
			}
		})
	}
}
