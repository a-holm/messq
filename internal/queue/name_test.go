// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

func TestValidateStreamName(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr error // nil means accept
	}{
		{"empty", "", errs.ErrBadRequest},
		{"plain", "orders", nil},
		{"charset", "orders_eu-2.x", nil},
		{"single char", "a", nil},
		{"max new length", strings.Repeat("a", 60), nil},
		{"over max new length", strings.Repeat("a", 61), errs.ErrBadRequest},
		{"non ascii", "ordér", errs.ErrBadRequest},
		{"space", "or ders", errs.ErrBadRequest},
		{"leading dot", ".orders", errs.ErrBadRequest},
		{"trailing dot", "orders.", errs.ErrBadRequest},
		{"double dot", "or..ders", errs.ErrBadRequest},
		{"reserved dlq", "orders.dlq", ErrReservedName},
		{"reserved dlq exact", ".dlq", ErrReservedName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStreamName(tt.in)
			switch {
			case tt.wantErr == nil && err != nil:
				t.Fatalf("ValidateStreamName(%q) = %v, want nil", tt.in, err)
			case tt.wantErr != nil && !errors.Is(err, tt.wantErr):
				t.Fatalf("ValidateStreamName(%q) = %v, want %v", tt.in, err, tt.wantErr)
			}
		})
	}
}
