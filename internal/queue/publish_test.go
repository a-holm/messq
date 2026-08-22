// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/errs"
)

func TestEncodeHeaders(t *testing.T) {
	l := DefaultLimits()

	t.Run("empty map encodes to empty string", func(t *testing.T) {
		got, err := EncodeHeaders(nil, l)
		if err != nil || got != "" {
			t.Fatalf("EncodeHeaders(nil) = %q, %v; want \"\", nil", got, err)
		}
	})

	t.Run("canonicalised and JSON-sorted", func(t *testing.T) {
		got, err := EncodeHeaders(map[string]string{"tenant-id": "acme", "Content-Type": "application/json"}, l)
		if err != nil {
			t.Fatalf("EncodeHeaders: %v", err)
		}
		var m map[string]string
		if jerr := json.Unmarshal([]byte(got), &m); jerr != nil {
			t.Fatalf("hdr is not JSON: %v (%q)", jerr, got)
		}
		if m["Tenant-Id"] != "acme" || m["Content-Type"] != "application/json" {
			t.Errorf("hdr = %s, want canonicalised keys", got)
		}
		if !strings.Contains(got, `"Content-Type"`) || strings.Index(got, `"Content-Type"`) > strings.Index(got, `"Tenant-Id"`) {
			t.Errorf("hdr keys not sorted: %s", got)
		}
	})

	t.Run("case-insensitive duplicate names are refused", func(t *testing.T) {
		_, err := EncodeHeaders(map[string]string{"tenant": "a", "Tenant": "b"}, l)
		if !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("duplicate canonical name = %v, want bad_request", err)
		}
	})

	t.Run("reserved namespace refused", func(t *testing.T) {
		var rhe *ReservedHeaderError
		_, err := EncodeHeaders(map[string]string{"messq-origin-stream": "orders"}, l)
		if !errors.As(err, &rhe) || !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("Messq-* header = %v, want ReservedHeaderError/bad_request", err)
		}
	})

	t.Run("value over 1 KiB", func(t *testing.T) {
		_, err := EncodeHeaders(map[string]string{"big": strings.Repeat("x", 1025)}, l)
		if !errors.Is(err, errs.ErrTooLarge) {
			t.Fatalf("1 KiB+1 value = %v, want too large", err)
		}
	})

	t.Run("total JSON over cap", func(t *testing.T) {
		h := map[string]string{}
		for i := range l.MaxHeaders {
			h["h"+string(rune('A'+i))] = strings.Repeat("y", 200)
		}
		_, err := EncodeHeaders(h, l)
		if !errors.Is(err, errs.ErrTooLarge) {
			t.Fatalf("aggregate over 4 KiB = %v, want too large", err)
		}
	})

	t.Run("too many headers", func(t *testing.T) {
		h := map[string]string{}
		for i := range l.MaxHeaders + 1 {
			h["k"+string(rune('a'+i/26))+string(rune('a'+i%26))] = "v"
		}
		_, err := EncodeHeaders(h, l)
		if !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("%d headers = %v, want bad_request", len(h), err)
		}
	})

	t.Run("invalid utf-8 value", func(t *testing.T) {
		_, err := EncodeHeaders(map[string]string{"bin": "\xff\xfe"}, l)
		if !errors.Is(err, errs.ErrBadRequest) {
			t.Fatalf("invalid UTF-8 value = %v, want bad_request", err)
		}
	})

	t.Run("exactly at caps passes", func(t *testing.T) {
		h := map[string]string{}
		for i := range l.MaxHeaders {
			key := "h" + strings.Repeat(string(rune('a'+i)), 8)
			h[key] = strings.Repeat("v", 100)
		}
		got, err := EncodeHeaders(h, l)
		if err != nil {
			t.Fatalf("32 small headers refused: %v", err)
		}
		if len(got) > l.MaxHeaderBytes {
			t.Errorf("hdr JSON %d bytes exceeds cap silently", len(got))
		}
	})
}

func TestValidatePublish(t *testing.T) {
	sc := DefaultConfig("orders") // subjects [">"], max_msg_size 1 MiB
	sc.Subjects = []string{"orders.*.created", "orders.eu.>"}
	l := DefaultLimits()
	const mib = int64(1) << 20

	tests := []struct {
		name    string
		req     PublishReq
		wantErr error
		checkAs func(*testing.T, error) // optional deeper type assertion
	}{
		{"ok", PublishReq{Subject: "orders.eu.created", Body: []byte("hi")}, nil, nil},
		{"matches star", PublishReq{Subject: "orders.us.created", Body: nil}, nil, nil},
		{
			"wildcard in published subject",
			PublishReq{Subject: "orders.*.created"},
			errs.ErrBadSubject,
			nil,
		},
		{
			"gt in published subject",
			PublishReq{Subject: "orders.eu.>"},
			errs.ErrBadSubject,
			nil,
		},
		{
			"no pattern match lists accepted",
			PublishReq{Subject: "invoices.created"},
			errs.ErrBadSubject,
			func(t *testing.T, err error) {
				var me *MismatchError
				if !errors.As(err, &me) {
					t.Fatalf("want MismatchError, got %v", err)
				}
				if len(me.Accepted) != 2 || me.Accepted[0] != "orders.*.created" {
					t.Errorf("accepted list = %v", me.Accepted)
				}
			},
		},
		{
			"body over stream max",
			PublishReq{Subject: "orders.eu.created", Body: make([]byte, mib+1)},
			errs.ErrTooLarge,
			func(t *testing.T, err error) {
				var te *TooLargeError
				if !errors.As(err, &te) {
					t.Fatalf("want TooLargeError, got %v", err)
				}
				if te.Size != mib+1 || te.Limit != mib {
					t.Errorf("TooLargeError = %+v", te)
				}
			},
		},
		{"empty body legal", PublishReq{Subject: "orders.eu.created"}, nil, nil},
		{"msg id ok", PublishReq{Subject: "orders.eu.created", MsgID: "order-4711-confirm"}, nil, nil},
		{"msg id empty string", PublishReq{Subject: "orders.eu.created", MsgID: ""}, nil, nil},
		{"msg id over 256 bytes", PublishReq{Subject: "orders.eu.created", MsgID: strings.Repeat("k", 257)}, errs.ErrBadRequest, nil},
		{"msg id non printable", PublishReq{Subject: "orders.eu.created", MsgID: "a\tb"}, errs.ErrBadRequest, nil},
		{"trace id ok", PublishReq{Subject: "orders.eu.created", TraceID: "4bf92f3577b34da6a3ce929d0e0e4736"}, nil, nil},
		{"trace id all zero", PublishReq{Subject: "orders.eu.created", TraceID: strings.Repeat("0", 32)}, errs.ErrBadRequest, nil},
		{"trace id whitespace", PublishReq{Subject: "orders.eu.created", TraceID: "abc def"}, errs.ErrBadRequest, nil},
		{"trace id too short", PublishReq{Subject: "orders.eu.created", TraceID: "abcd"}, errs.ErrBadRequest, nil},
		{"headers over cap", PublishReq{Subject: "orders.eu.created", Headers: map[string]string{"h": strings.Repeat("v", 5000)}}, errs.ErrTooLarge, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePublish(sc, tt.req, l)
			switch {
			case tt.wantErr == nil && err != nil:
				t.Fatalf("ValidatePublish() = %v, want nil", err)
			case tt.wantErr != nil && !errors.Is(err, tt.wantErr):
				t.Fatalf("ValidatePublish() = %v, want %v", err, tt.wantErr)
			}
			if tt.checkAs != nil {
				tt.checkAs(t, err)
			}
		})
	}
}
