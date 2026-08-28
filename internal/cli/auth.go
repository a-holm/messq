// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/a-holm/messq/internal/auth"
	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/spf13/cobra"
)

// The `messq auth` command family (issue #16 slice 11): mint and inspect bearer
// credentials without ever letting a secret touch argv, an environment variable
// or terminal scrollback more than the sanctioned once-on-stdout window. The
// credential is the whole presented string `msq1_<id>_<secret>` — prefix and id
// included in what the stored SHA-256 covers (issue body decision 2) — so
// `messq auth hash` hashes stdin verbatim modulo one documented newline rule.

// crockfordAlphabet is Crockford base32 without padding or check digits: an
// unambiguous, copy-paste-safe encoding whose characters all sit inside the
// credential alphabet [A-Za-z0-9._~-].
const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// secretEntropyBytes mints 256-bit secrets: 32 bytes encode to ~52 Crockford
// characters, comfortably above the server-side minimum of 16.
const secretEntropyBytes = 32

// minSecretChars mirrors the credential shape floor so an operator can eyeball
// that what got minted looks like what the daemon will accept.
const minSecretChars = 52

// newAuthCmd assembles the auth command group.
func newAuthCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "manage bearer tokens for daemon authentication",
		Long: "Create, list and verify the bearer credentials a messq daemon reads from\n" +
			"its --auth-file. Secrets are generated at cryptographic strength on this\n" +
			"machine, shown exactly once on stdout, and never accepted from argv or\n" +
			"environment variables: a credential that entered shell history is a\n" +
			"credential you must rotate.",
		Example: "  messq auth add --auth-file /etc/messq/tokens --id ci --roles publish,consume --streams 'orders*'\n" +
			"  messq auth ls --auth-file /etc/messq/tokens\n" +
			"  printf '%s' \"$CRED\" | messq auth check --auth-file /etc/messq/tokens",
		GroupID: "manage",
		Args:    exactArgsMessage,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAuthAddCmd(env), newAuthHashCmd(env), newAuthLsCmd(env), newAuthCheckCmd(env))
	return cmd
}

func newAuthAddCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add",
		Short: "mint one token, print its credential once, store its hash",
		Long: "Mint a fresh credential msq1_<id>_<secret>, append its SHA-256 line to the\n" +
			"--auth-file, and print the credential to stdout EXACTLY ONCE. Nobody can show\n" +
			"it again; if it is lost, remove the line by hand and add anew with the same\n" +
			"id — renaming ids invalidates credentials by design (the hash covers them).\n" +
			"The file keeps mode 0600 and single-line appends leave every prior token\n" +
			"intact. Duplicate ids and duplicate hashes are refused before anything is\n" +
			"written: two ids sharing one credential would make events.actor ambiguous.\n" +
			"Roles are publish, consume and admin — plain sets, no hierarchy — scoped to\n" +
			"comma-separated stream patterns ('orders' is exact, 'orders*' covers the\n" +
			"family including orders.dlq).",
		Example: "  messq auth add --auth-file /etc/messq/tokens \\\n" +
			"      --id ci-worker --roles publish,consume --streams 'orders*,billing'",
		Args:        noSurplusArgs,
		Annotations: map[string]string{annExits: "0,2,4"},
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, ferr := resolvedAuthFile(cmd)
			if ferr != nil {
				return ferr
			}
			id := flagString(cmd, "id")
			rolesStr := flagString(cmd, "roles")
			streamsStr := flagString(cmd, "streams")
			if missing := missingAuthAddFlags(id, rolesStr, streamsStr); len(missing) > 0 {
				return uierr.Usage("auth add needs %s", strings.Join(missing, ", "))
			}

			tok, credential, terr := buildTokenForAdd(id, rolesStr, streamsStr)
			if terr != nil {
				return uierr.Usage("%s", terr.Error())
			}

			existing, err := readAuthTokens(path)
			if err != nil && !os.IsNotExist(err) {
				return &uierr.UserError{Code: "usage", Summary: err.Error(), Next: []string{"inspect the named token file"}, Exit: exitUsage}
			}
			for _, t := range existingTokens(existing) {
				if t.ID == tok.ID {
					return &uierr.UserError{
						Code:    "conflict",
						Summary: fmt.Sprintf("duplicate token id %q in %s", tok.ID, path),
						Because: "token ids identify the actor in logs and events; two entries would blur who acted.",
						Next:    []string{fmt.Sprintf("messq auth ls --auth-file %q", path)},
						Exit:    exitConflict,
					}
				}
				if t.Hash == tok.Hash {
					return &uierr.UserError{
						Code:    "conflict",
						Summary: fmt.Sprintf("this credential's hash already exists in %s under id %q", path, t.ID),
						Because: "two ids sharing one credential makes the audit trail ambiguous.",
						Next:    []string{fmt.Sprintf("messq auth ls --auth-file %q", path)},
						Exit:    exitConflict,
					}
				}
			}

			f, werr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
			if werr != nil {
				return writeFailed(werr)
			}
			_, werr = f.WriteString(tok.String() + "\n")
			serr := f.Sync()
			cerr := f.Close()
			if werr != nil || serr != nil || cerr != nil {
				return writeFailed(firstErr(werr, serr, cerr))
			}

			out := env.stdoutOrDiscard()
			fmt.Fprintln(out, credential)
			_, _ = fmt.Fprintf(env.stderr(),
				"stored sha256 line for id %q in %s\nnobody can print this credential again — store it where your deploy tooling reads it.\n",
				tok.ID, path)
			return nil
		},
	}
	fs := cmd.Flags()
	fs.String("auth-file", "", "token file to append to (or MESSQ_AUTH_FILE)")
	fs.String("id", "", "token id: [a-z0-9][a-z0-9._-]{1,63}")
	fs.String("roles", "", "comma-separated subset of publish, consume, admin")
	fs.String("streams", "", "comma-separated stream patterns (exact, prefix*, or *)")
	return cmd
}

func newAuthHashCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hash",
		Short: "hash a credential from stdin into the auth-file form",
		Long: "Read ONE credential from stdin and print the SHA-256 hex form an auth-file\n" +
			"line carries. Hashes cover the WHOLE presented string including the msq1_\n" +
			"prefix and the id — there is no second parse of a credential anywhere in\n" +
			"messq. Newline rule: exactly one trailing LF (and one CR if CRLF) is\n" +
			"stripped; everything else hashes byte-for-byte. That means `echo $cred`\n" +
			"(which appends a newline) hashes DIFFERENTLY from `printf '%s' $cred`;\n" +
			"tests pin both digests so scripts can rely on either stated recipe.",
		Example: "  printf '%s' \"$MESSQ_TOKEN\" | messq auth hash\n" +
			"  messq auth hash < token-copy.txt   # one trailing newline tolerated",
		Args:        noSurplusArgs,
		Annotations: map[string]string{annExits: "0,1,2"},
		RunE: func(_ *cobra.Command, _ []string) error {
			data, err := io.ReadAll(env.Stdin)
			if err != nil {
				return failure(exitError, "cannot read stdin: %v", err)
			}
			trimmed, empty := trimCredentialInput(string(data))
			if empty {
				return uierr.Usage(`no credential on stdin; use printf '%%s' "$TOKEN" | messq auth hash`)
			}
			sum := sha256.Sum256([]byte(trimmed))
			fmt.Fprintln(env.stdoutOrDiscard(), hex.EncodeToString(sum[:]))
			return nil
		},
	}
	return cmd
}

func newAuthLsCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "list token ids, roles and stream patterns — never hashes",
		Long: "List every token in the auth-file with its id, role set and stream patterns.\n" +
			"The digest column is deliberately absent: someone holding only a stored\n" +
			"hash could replay requests offline against any endpoint that accepts the\n" +
			"presented form, so listings prove nothing beyond WHAT each id may do.\n" +
			"Editing grants means editing the file line; the daemon picks the change up\n" +
			"when the reload seam lands with issue #17.",
		Example: "  messq auth ls --auth-file /etc/messq/tokens\n" +
			"  messq auth ls --output json | jq '.tokens[].id'",
		Args:        noSurplusArgs,
		Annotations: map[string]string{annExits: "0,2,3"},
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, ferr := resolvedAuthFile(cmd)
			if ferr != nil {
				return ferr
			}
			file, rerr := readAuthTokens(path)
			if rerr != nil {
				return &uierr.UserError{
					Code:    "not_found",
					Summary: rerr.Error(),
					Next:    []string{"messq auth add --auth-file " + path + " --id <id> --roles <roles> --streams '<patterns>'"},
					Exit:    exitNotFound,
				}
			}
			format := render.FormatTable
			if s := sessionFrom(cmd); s != nil {
				format = s.format
			}
			out := env.stdoutOrDiscard()

			type row struct {
				ID      string   `json:"id"`
				Roles   []string `json:"roles"`
				Streams []string `json:"streams"`
			}
			rows := make([]row, 0, len(file.Tokens))
			for _, t := range file.Tokens {
				rows = append(rows, row{ID: t.ID, Roles: splitRoles(t.Roles.String()), Streams: patternStrings(t)})
			}
			sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

			if format == render.FormatJSON || format == render.FormatNDJSON {
				enc := json.NewEncoder(out)
				enc.SetIndent("", "  ")
				if encErr := enc.Encode(struct {
					Tokens []row `json:"tokens"`
				}{rows}); encErr != nil {
					return failure(exitError, "render json: %v", encErr)
				}
				return nil
			}
			w := tabwriter.NewWriter(out, 0, 2, 2, ' ', 0)
			fmt.Fprintln(w, "ID	ROLES	STREAMS")
			for _, r := range rows {
				fmt.Fprintf(w, "%s	%s	%s\n", r.ID, strings.Join(r.Roles, ","), strings.Join(r.Streams, ","))
			}
			return w.Flush()
		},
	}
	cmd.Flags().String("auth-file", "", "token file to inspect (or MESSQ_AUTH_FILE)")
	return cmd
}

func newAuthCheckCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check",
		Short: "verify a credential from stdin against an auth-file",
		Long: "Check whether a credential matches SOME entry of the named auth-file and\n" +
			"exit 0 on match, 1 on mismatch — the yes/no shape deployment jobs want\n" +
			"before they ship a rotated secret. The credential arrives on stdin whole,\n" +
			"one trailing LF/CRLF stripped exactly like `messq auth hash`. A mismatch\n" +
			"always names the usual suspects, including the DLQ nuance: an exact grant\n" +
			"like 'orders' never covers 'orders.dlq', while a trailing 'orders*' does.",
		Example:     "  printf '%s' \"$NEW_CRED\" | messq auth check --auth-file /etc/messq/tokens && echo rotate-ok",
		Args:        noSurplusArgs,
		Annotations: map[string]string{annExits: "0,1,2"},
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, ferr := resolvedAuthFile(cmd)
			if ferr != nil {
				return ferr
			}
			data, rerr := io.ReadAll(env.Stdin)
			if rerr != nil {
				return failure(exitError, "cannot read stdin: %v", rerr)
			}
			credential, empty := trimCredentialInput(string(data))
			if empty {
				return uierr.Usage(`no credential on stdin; use printf '%%s' "$CRED" | messq auth check --auth-file <path>`)
			}
			file, lerr := readAuthTokens(path)
			if lerr != nil {
				return &uierr.UserError{Code: "not_found", Summary: lerr.Error(), Exit: exitNotFound}
			}
			registry := auth.NewRegistry(file.Tokens)
			principal, verr := registry.Verify(credential)

			if verr == nil {
				fmt.Fprintf(env.stdoutOrDiscard(), "ok %s roles=%s\n", principal.ID, rolesOfEntry(file, credential))
				return nil
			}
			return &uierr.UserError{
				Code:    "unauthorized",
				Summary: "credential does not match any entry of " + path + "; note that an exact grant like 'orders' does not cover 'orders.dlq' while trailing-'orders*' does",
				Because: "usual causes: rotated here but not deployed elsewhere; a trailing newline changed the hash.",
				Next:    []string{"messq auth add --auth-file " + path + " --id <new-id> --roles <roles> --streams '<patterns>'"},
				Exit:    exitError,
			}
		},
	}
	cmd.Flags().String("auth-file", "", "token file to verify against (or MESSQ_AUTH_FILE)")
	return cmd
}

// ---------------------------------------------------------------------------
// Shared helpers.
// ---------------------------------------------------------------------------

// trimCredentialInput strips at most one trailing LF preceded optionally by CR.
// The echo-vs-printf divergence is DOCUMENTED behaviour pinned by tests.
func trimCredentialInput(s string) (string, bool) {
	s = strings.TrimSuffix(s, "\n")
	s = strings.TrimSuffix(s, "\r")
	return s, s == ""
}

// resolvedAuthFile resolves --auth-file flag → MESSQ_AUTH_FILE, matching serve.
func resolvedAuthFile(cmd *cobra.Command) (string, error) {
	v := flagString(cmd, "auth-file")
	if v == "" {
		if s := sessionFrom(cmd); s != nil && s.env.Getenv != nil {
			v = s.env.Getenv("MESSQ_AUTH_FILE")
		}
	}
	if strings.TrimSpace(v) == "" {
		return "", uierr.Usage("auth commands need --auth-file (or MESSQ_AUTH_FILE)")
	}
	return v, nil
}

func flagString(cmd *cobra.Command, name string) string {
	v, err := cmd.Flags().GetString(name)
	if err != nil {
		return "" // only possible for flags this file registered
	}
	return v
}

func missingAuthAddFlags(id, roles, streams string) []string {
	var missing []string
	if id == "" {
		missing = append(missing, "--id")
	}
	if roles == "" {
		missing = append(missing, "--roles")
	}
	if streams == "" {
		missing = append(missing, "--streams")
	}
	return missing
}

// noSurplusArgs refuses ANY positional argument. For auth commands a stray word
// is most plausibly a pasted credential arriving where flags belong — refused as
// usage before anything touches disk or prints.
func noSurplusArgs(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return uierr.Usage("credentials ride stdin or flags only, never positional arguments (#16): got %d unexpected argument(s)", len(args))
	}
	return cobra.NoArgs(cmd, args)
}

// buildTokenForAdd validates operator input, mints the random half, and returns
// BOTH the storable token (hash included) and the credential itself for the one-
// time stdout emission. Validation errors are usage errors teaching grammar.
func buildTokenForAdd(id, rolesStr, streamsStr string) (auth.Token, string, error) {
	if !validCLIAuthID(id) {
		return auth.Token{}, "", fmt.Errorf("--id %q must match [a-z0-9][a-z0-9._-]{1,63}", id)
	}
	secret, err := mintSecret()
	if err != nil {
		return auth.Token{}, "", fmt.Errorf("mint secret: %w", err)
	}
	credential := "msq1_" + id + "_" + secret
	hash := sha256.Sum256([]byte(credential))
	tok := auth.Token{ID: id, Hash: hash}
	rs, rerr := auth.ParseRoleSet(rolesStr)
	if rerr != nil {
		return auth.Token{}, "", fmt.Errorf("--roles: %w", rerr)
	}
	tok.Roles = rs
	for _, s := range strings.Split(streamsStr, ",") {
		p, perr := auth.ParsePattern(s)
		if perr != nil {
			return auth.Token{}, "", fmt.Errorf("--streams: %w", perr)
		}
		tok.Patterns = append(tok.Patterns, p)
	}
	return tok, credential, nil
}

// mintSecret returns 256 bits of crypto/rand entropy encoded as Crockford base32
// (52 characters): unambiguous when typed by hand, and every character sits in
// the credential alphabet the server's shape check accepts.
func mintSecret() (string, error) {
	raw := make([]byte, secretEntropyBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	n := new(big.Int).SetBytes(raw)
	base := big.NewInt(int64(len(crockfordAlphabet)))
	mod := new(big.Int)
	digits := make([]byte, 0, 64)
	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		digits = append(digits, crockfordAlphabet[mod.Int64()])
	}
	for len(digits) < minSecretChars {
		digits = append(digits, crockfordAlphabet[0])
	}
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	return string(digits), nil
}

func validCLIAuthID(s string) bool {
	if len(s) < 2 || len(s) > 64 {
		return false
	}
	okLead := func(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') }
	if !okLead(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if !okLead(c) && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// rolesOfEntry reports the role column of the entry whose full-credential hash
// matches the presented credential. Registry.Verify already established a match;
// this re-finds the row purely for the human summary line.
func rolesOfEntry(file *auth.File, credential string) string {
	sum := sha256.Sum256([]byte(credential))
	for _, t := range file.Tokens {
		if t.Hash == sum {
			return t.Roles.String()
		}
	}
	return ""
}

func existingTokens(f *auth.File) []auth.Token {
	if f == nil {
		return nil
	}
	return f.Tokens
}

func readAuthTokens(path string) (*auth.File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return auth.Parse(path, strings.NewReader(string(data)))
}

func splitRoles(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

func patternStrings(t auth.Token) []string {
	out := make([]string, 0, len(t.Patterns))
	for _, p := range t.Patterns {
		out = append(out, p.String())
	}
	return out
}

func writeFailed(err error) error {
	return &uierr.UserError{
		Code:    "io",
		Summary: "could not update the token file",
		Because: err.Error(),
		Next:    []string{"check that the directory exists and is writable by this user"},
		Exit:    exitError,
	}
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func failure(code int, format string, args ...any) error {
	return &uierr.UserError{Code: "error", Summary: fmt.Sprintf(format, args...), Exit: code}
}
