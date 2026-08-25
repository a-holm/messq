// SPDX-License-Identifier: Apache-2.0

package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// canary is the secret no rendering may ever leak.
const canary = "supersecret-part-9f3a"

func TestCredentialRedactsEveryInterface(t *testing.T) {
	t.Parallel()

	cred := TokenCredential("msq1_id7_" + canary)

	check := func(what, got string) {
		t.Helper()
		if strings.Contains(got, canary) {
			t.Errorf("%s leaked the secret: %q", what, got)
		}
	}

	// Stringer + Formatter across verbs.
	check("String()", cred.String())
	for _, verb := range []string{"%v", "%s", "%q", "%x", "%d", "%+v", "%#v"} {
		check("Format("+verb+")", fmt.Sprintf(verb, cred))
	}

	// MarshalText (encoding.TextMarshaler — also drives map keys and encoding/xml).
	b, err := cred.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	check("MarshalText()", string(b))

	// MarshalJSON.
	j, err := json.Marshal(cred)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	check("MarshalJSON()", string(j))
	var roundTrip struct{ C Credential }
	if err := json.Unmarshal([]byte(`{"c":"msq1_x_y"}`), &roundTrip); err != nil || roundTrip.C.token != "msq1_x_y" {
		t.Fatalf("Unmarshal into Credential: %v %+v", err, roundTrip)
	}

	// slog.LogValue.
	lv := cred.LogValue()
	if s := lv.String(); strings.Contains(s, canary) {
		t.Errorf("LogValue() = %q, want a redacted string", s)
	}
	buf := &strings.Builder{}
	logger := slog.New(slog.NewJSONHandler(buf, nil))
	logger.Info("creds", "cred", cred)
	check("slog output", buf.String())

	// The redaction keeps the id visible for support conversations.
	if got := cred.String(); got != "msq1_id7_***" {
		t.Errorf("String() = %q, want msq1_id7_***", got)
	}
}

func TestCredentialFromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "token")

	if err := os.WriteFile(path, []byte("  msq1_ops_"+canary+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cred, err := CredentialFromFile(path)
	if err != nil {
		t.Fatalf("CredentialFromFile: %v", err)
	}
	if cred.token != "msq1_ops_"+canary {
		t.Errorf("token = %q, want the whole trimmed file content", cred.token)
	}

	// The predictable mistake: handing over the SERVER's --auth-file (whose lines
	// are id/hash/roles/streams — four whitespace-separated fields).
	serverFile := filepath.Join(dir, "auth-file")
	if werr := os.WriteFile(serverFile,
		[]byte("# comment\nops	2bb80d537b1a3f0fa4b1c8a5e4e0f2a1	writer	orders.*\n"), 0o600); werr != nil {
		t.Fatal(werr)
	}
	_, confusionErr := CredentialFromFile(serverFile)
	if confusionErr == nil {
		t.Fatal("the server auth-file was accepted as a client credential")
	}
	msg := confusionErr.Error()
	for _, want := range []string{"--auth-file", serverFile} {
		if !strings.Contains(msg, want) {
			t.Errorf("confusion error %q does not name %q", msg, want)
		}
	}

	// An empty credential file is refused.
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := CredentialFromFile(empty); !errors.Is(err, ErrConfig) {
		t.Errorf("empty file: err = %v, want ErrConfig", err)
	}
}

func TestEnvHelpersReadExplicitly(t *testing.T) {
	t.Setenv("MESSQ_TOKEN", "msq1_e_f")
	t.Setenv("MESSQ_ADDR", "unix:///run/messq/messq.sock")

	cred, ok := TokenFromEnv()
	if !ok || cred.token != "msq1_e_f" {
		t.Errorf("TokenFromEnv() = %v, %v", cred, ok)
	}
	addr, ok := AddrFromEnv()
	if !ok || addr != "unix:///run/messq/messq.sock" {
		t.Errorf("AddrFromEnv() = %q, %v", addr, ok)
	}

	t.Setenv("MESSQ_TOKEN", "")
	if _, ok := TokenFromEnv(); ok {
		t.Error("empty MESSQ_TOKEN reported present")
	}
}
