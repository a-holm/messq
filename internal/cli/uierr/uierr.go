// SPDX-License-Identifier: Apache-2.0

// Package uierr is the teaching-error type and its two renderings (issue §6): the
// human face — what happened / why / what to type next — and the machine face, where
// the daemon's error envelope is re-emitted on stderr so `messq … -o json | jq`
// never receives an error object in its data stream.
//
// The no-invented-text rule is the reason this is a type and not an fmt.Errorf: when
// the daemon returned an envelope, Summary IS client.Error.Message, Next IS
// client.Error.Next and Detail IS client.Error.Detail, byte-for-byte. The CLI adds
// only what the daemon cannot know: the address it dialled, the flag the user typed,
// locally computed did-you-mean candidates, and a help topic.
package uierr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/pkg/client"
)

// UserError is one failure an operator can act on. Exit == 0 means "ask the exit
// classifier"; anything else is a command's own documented outcome.
type UserError struct {
	Code    string         // wire code from the envelope, or a CLI-local code
	Summary string         // one line: what happened (daemon's sentence, verbatim)
	Because string         // optional: why / the concept, 1–3 lines
	Suggest []string       // "did you mean" candidates, computed locally
	Next    []string       // commands to type; server-supplied entries verbatim
	Help    string         // e.g. "messq help concepts" (topics land in #26)
	Detail  map[string]any // machine-readable specifics, from envelope.detail
	TraceID string
	Exit    int
	Cause   error
}

func (e *UserError) Error() string {
	if e.Summary != "" {
		return e.Summary
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Code
}

func (e *UserError) ExitCode() int { return e.Exit }

func (e *UserError) Unwrap() error { return e.Cause }

// FromClient adapts a daemon envelope. Message/Next/Detail/TraceID are copied
// byte-for-byte; addr (the address actually dialled) may be appended to the rendered
// output but never rewrites a suggestion.
func FromClient(ce *client.Error, addr string) *UserError {
	ue := &UserError{
		Code:    ce.Code,
		Summary: ce.Message,
		Next:    append([]string(nil), ce.Next...),
		Detail:  ce.Detail,
		TraceID: ce.TraceID,
		Cause:   ce,
	}
	if len(ue.Next) == 0 { // local fallback teaching line for envelopes without one
		ue.Next = []string{"messq --help"}
	}
	if addr != "" && ce.Status == 0 {
		ue.Because = "the daemon at " + addr + " could not answer this request."
	}
	return ue
}

// Usage builds a CLI-local usage failure: bad flag, bad argument, bad flag-value.
// The message names what to fix; Next always carries something to type.
func Usage(format string, args ...any) *UserError {
	return &UserError{
		Code:    "usage",
		Summary: fmt.Sprintf(format, args...),
		Next:    []string{"messq --help"},
		Exit:    2,
	}
}

// Env carries the rendering context: which stream receives the error, which face the
// invocation resolved to, and whether stderr is a terminal.
type Env struct {
	Stderr io.Writer
	Format render.Format
}

// Render writes ONE error rendering, once, to stderr — human shape for table mode,
// the envelope shape for machine modes. Errors are never written to stdout.
func Render(env *Env, err error, code int) {
	if env == nil || env.Stderr == nil || err == nil {
		return
	}
	var ue *UserError
	if !errors.As(err, &ue) {
		ue = fromPlain(err, code)
	}
	if env.Format == render.FormatJSON || env.Format == render.FormatNDJSON {
		renderMachine(env.Stderr, ue, code)
		return
	}
	renderHuman(env.Stderr, ue)
}

// fromPlain wraps an untyped failure with the CLI-local code vocabulary so even a
// raw Go error gets a well-formed envelope.
func fromPlain(err error, code int) *UserError {
	ue := &UserError{
		Code:    localCode(code),
		Summary: err.Error(),
		Next:    []string{"messq --help"},
		Cause:   err,
	}
	return ue
}

func localCode(code int) string {
	switch code {
	case 2:
		return "usage"
	case 5:
		return "wait_expired"
	case 7:
		return "token_file_perms"
	case 130:
		return "interrupted"
	default:
		return "unreachable"
	}
}

// renderHuman draws the what / why / next / learn-more face directly from PLAN §8.
// Everything printed comes from the type's fields — the renderer adds layout only.
func renderHuman(w io.Writer, ue *UserError) {
	var b strings.Builder
	fmt.Fprintf(&b, "Error: %s\n", render.Safe(ue.Error()))
	if ue.Because != "" {
		fmt.Fprintf(&b, "\n  %s\n", render.Safe(ue.Because))
	}
	if len(ue.Suggest) > 0 {
		fmt.Fprintf(&b, "\n  Did you mean:  %s\n", strings.Join(ue.Suggest, ", "))
	}
	for _, n := range ue.Next {
		fmt.Fprintf(&b, "\n  %s", n)
	}
	if len(ue.Next) == 1 {
		b.WriteString("\n")
	}
	if ue.Help != "" {
		fmt.Fprintf(&b, "\n  Learn more:  %s\n", ue.Help)
	}
	fmt.Fprint(w, b.String())
}

// renderMachine re-emits the envelope on stderr. A daemon envelope keeps its fields
// verbatim; a CLI-local failure synthesises the same shape with a CLI-local code.
func renderMachine(w io.Writer, ue *UserError, code int) {
	inner := map[string]any{
		"code":    ue.Code,
		"message": ue.Summary,
		"next":    ue.Next,
	}
	if ue.Detail != nil {
		inner["detail"] = ue.Detail
	}
	if ue.TraceID != "" {
		inner["trace_id"] = ue.TraceID
	}
	if code != 0 && ue.Cause != nil {
		inner["exit"] = code
	}
	line, err := json.Marshal(map[string]any{"error": inner})
	if err != nil {
		// A map of strings cannot fail to marshal; if one ever does, the plain
		// sentence is still better than silence.
		fmt.Fprintf(w, "{\"error\":{\"code\":%q,\"message\":%q}}\n", ue.Code, ue.Summary)
		return
	}
	fmt.Fprintln(w, string(line))
}
