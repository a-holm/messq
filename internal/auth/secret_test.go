// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/auth"
)

const canarySecret = "msq1_canary-tok_CANARY-SECRET-VALUE-0123456789"

// TestSecretNeverLeaksThroughFormatting is the unit half of the canary test: every formatting
// verb and the String/GoString methods yield no secret bytes. The flag is the canary itself; a
// hit means a formatting path reached the secret.
func TestSecretNeverLeaksThroughFormatting(t *testing.T) {
	t.Parallel()

	s := auth.Secret(canarySecret)

	tests := []struct {
		name string
		got  string
	}{
		{name: "%v", got: fmt.Sprintf("%v", s)},
		{name: "%s", got: fmt.Sprintf("token=%s", s)},
		{name: "%+v", got: fmt.Sprintf("%+v", s)},
		{name: "%#v", got: fmt.Sprintf("%#v", s)},
		{name: "String()", got: s.String()},
		{name: "struct sprint", got: fmt.Sprint(struct{ S auth.Secret }{S: s})},
		{name: "struct %+v", got: fmt.Sprintf("%+v", struct{ S auth.Secret }{S: s})},
	}
	for _, tc := range tests {
		if strings.Contains(tc.got, "CANARY-SECRET-VALUE") {
			t.Errorf("%s leaked the secret: %q", tc.name, tc.got)
		}
		if !strings.Contains(tc.got, "REDACTED") {
			t.Errorf("%s = %q, want the REDACTED marker", tc.name, tc.got)
		}
	}
}

// TestSecretRevealIsTheSingleExit pins that Reveal returns the secret unchanged, so the one
// greppable exit is exactly where the secret is meant to become readable.
func TestSecretRevealIsTheSingleExit(t *testing.T) {
	t.Parallel()

	s := auth.Secret(canarySecret)
	if got := s.Reveal(); got != canarySecret {
		t.Errorf("Reveal() = %q, want the secret itself", got)
	}
}

// TestSecretNeverReachesJSON pins MarshalJSON and MarshalText.
func TestSecretNeverReachesJSON(t *testing.T) {
	t.Parallel()

	s := auth.Secret(canarySecret)

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("json.Marshal(secret) error = %v", err)
	}
	if string(b) != `"REDACTED"` {
		t.Errorf("json.Marshal(secret) = %s, want %q", b, `"REDACTED"`)
	}

	// A struct containing a Secret marshals with the field redacted too.
	got, err := json.Marshal(struct {
		Token auth.Secret `json:"token"`
	}{Token: s})
	if err != nil {
		t.Fatalf("json.Marshal(struct) error = %v", err)
	}
	if strings.Contains(string(got), "CANARY-SECRET-VALUE") {
		t.Errorf("json.Marshal(struct) leaked the secret: %s", got)
	}

	text, err := s.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText error = %v", err)
	}
	if string(text) != "REDACTED" {
		t.Errorf("MarshalText = %q, want REDACTED", text)
	}
}

// TestSecretNeverReachesSlog pins that both the text and JSON slog handlers redact the secret,
// at DEBUG level, through slog.Any (which routes to LogValue).
func TestSecretNeverReachesSlog(t *testing.T) {
	t.Parallel()

	s := auth.Secret(canarySecret)

	if got := s.LogValue().String(); got != "REDACTED" {
		t.Errorf("LogValue().String() = %q, want REDACTED", got)
	}

	for _, name := range []string{"text", "json"} {
		var buf bytes.Buffer
		var h slog.Handler
		switch name {
		case "text":
			h = slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
		case "json":
			h = slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
		}
		logger := slog.New(h)
		logger.Debug("auth", "secret", s, "actor", "tok:canary-tok")

		if strings.Contains(buf.String(), "CANARY-SECRET-VALUE") {
			t.Errorf("%s handler leaked the secret: %s", name, buf.String())
		}
		if !strings.Contains(buf.String(), "REDACTED") {
			t.Errorf("%s handler = %s, want the REDACTED marker", name, buf.String())
		}
	}
}
