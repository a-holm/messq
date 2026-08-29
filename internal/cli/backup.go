// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/a-holm/messq/internal/clock"
	"github.com/a-holm/messq/internal/ops/backup"
	"github.com/spf13/cobra"
)

// seamClock delegates everything but Now to the real system clock: durations
// stay monotonic while every persisted timestamp rides the Env's frozen-now
// seam (#3's rule applied at the command boundary).
type seamClock struct {
	clock.System
	now func() time.Time
}

func (c seamClock) Now() time.Time { return c.now() }

// backupView is messq backup's View (§8): one receipt, three faces, frozen
// field names. Zero I/O after Run returns.
type backupView struct {
	res   *backup.Result
	hints []Hint
}

func (v backupView) ExitCode() int { return exit.OK }

func (v backupView) Hints() []Hint { return v.hints }

// NDJSON is nil by the View contract: a scalar receipt renders its JSON face
// as a single line, exactly as `--output ndjson` shows it.
func (v backupView) NDJSON() []any { return nil }

// backupJSONDoc is the frozen --output json face. Field names are compatibility
// surface from #30 onward; renaming any of them breaks scripts.
type backupJSONDoc struct {
	Schema             int              `json:"schema"`
	Dest               string           `json:"dest"`
	Bytes              int64            `json:"bytes"`
	Pages              int64            `json:"pages"`
	TakenAt            string           `json:"taken_at"`
	DurationMS         int64            `json:"duration_ms"`
	Verified           string           `json:"verified"`
	SourceNodeID       string           `json:"source_node_id"`
	StreamHeads        map[string]int64 `json:"stream_heads"`
	InflightAtSnapshot int64            `json:"inflight_at_snapshot"`
	SweptStaleTempDirs int              `json:"swept_stale_temp_dirs"`
}

const backupDocSchema = 1

func (v backupView) JSON() any {
	r := v.res
	return backupJSONDoc{
		Schema:             backupDocSchema,
		Dest:               r.Dest,
		Bytes:              r.Bytes,
		Pages:              r.Pages,
		TakenAt:            r.TakenAt.UTC().Format(time.RFC3339),
		DurationMS:         r.Duration.Milliseconds(),
		Verified:           r.Verified,
		SourceNodeID:       r.SourceNodeID,
		StreamHeads:        r.StreamHeads,
		InflightAtSnapshot: r.InflightAtSnapshot,
		SweptStaleTempDirs: r.Swept,
	}
}

// Table writes the human receipt: what was produced and how it was proven.
func (v backupView) Table(w io.Writer) error {
	r := v.res
	tw := render.NewTableWriter(w)
	rows := [][2]string{
		{"dest", r.Dest},
		{"bytes", fmt.Sprintf("%s (%d pages)", render.Count(r.Bytes), r.Pages)},
		{"verified", r.Verified},
		{"taken-at", r.TakenAt.UTC().Format(time.RFC3339)},
		{"duration", formatDurationReceipt(r.Duration)},
		{"node", render.Safe(r.SourceNodeID)},
	}
	for _, row := range rows {
		if err := tw.WriteLine(row[0] + "\t" + row[1]); err != nil {
			return err
		}
	}
	if len(r.StreamHeads) > 0 {
		if err := tw.WriteLine("heads\t" + formatHeads(r.StreamHeads)); err != nil {
			return err
		}
	}
	if err := tw.WriteLine(fmt.Sprintf("swept\t%d stale temp dirs", r.Swept)); err != nil {
		return err
	}
	return tw.Flush()
}

// formatDurationReceipt renders wall time for humans without pretending
// precision: whole milliseconds below a second, tenths above it.
func formatDurationReceipt(d time.Duration) string {
	if d >= time.Second {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// formatHeads renders per-stream heads sorted so receipts diff cleanly.
func formatHeads(heads map[string]int64) string {
	names := make([]string, 0, len(heads))
	for name := range heads {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = fmt.Sprintf("%s=%d", render.Safe(name), heads[name])
	}
	return strings.Join(parts, " ")
}

// newBackupCmd wires `messq backup <dest>` onto the chassis (#23): all refusals
// happen before any page is copied, each carrying its teaching error and
// documented exit code (4 exists-without-force, 2 inside-the-data-dir/usage,
// 7 unwritable), then a self-checked snapshot lands in the resolved face.
func newBackupCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup <dest>",
		Short: "snapshot a data directory with VACUUM INTO",
		Long: "Take a consistent, self-verifying snapshot of a messq data directory " +
			"into a single file via SQLite's VACUUM INTO on a read-only connection.\n" +
			"\n" +
			"The snapshot works while the daemon runs AND after it stops, is written " +
			"to a private temp directory, and renamed into place only after its own " +
			"self-check passes. Provenance is stamped into the file itself so it " +
			"explains its origin to whoever restores it later.",
		Example: "  messq backup /var/backups/messq/$(date -u +%FT%H%MZ).db --data-dir /var/lib/messq\n" +
			"  messq backup /tmp/snap.db --data-dir /var/lib/messq --verify full --force",
		GroupID: "operate",
		Args:    exactlyOneBackupDest,
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir := cmd.Flags().Lookup("data-dir").Value.String()
			if dataDir == "" {
				return uierr.Usage("--data-dir is required (or set MESSQ_DATA_DIR)")
			}
			force, gErr := cmd.Flags().GetBool("force")
			if gErr != nil {
				return uierr.Usage("%s", gErr)
			}
			verify, vErr := cmd.Flags().GetString("verify")
			if vErr != nil {
				return uierr.Usage("%s", vErr)
			}
			if parseVerifyMode(verify) == backup.VerifyMode(255) {
				return uierr.Usage("invalid --verify %q: want quick|full|none", verify)
			}

			opts := backup.Options{
				DataDir: dataDir,
				Dest:    args[0],
				Force:   force,
				Verify:  parseVerifyMode(verify),
				Clock:   seamClock{now: env.Now},
			}
			format := render.FormatTable
			if s := sessionFrom(cmd); s != nil {
				format = s.format
			}

			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			plan, err := backup.Plan(ctx, opts)
			if err != nil {
				return teachBackupRefusal(err)
			}
			res, runErr := backup.Run(ctx, plan)
			if runErr != nil {
				return teachBackupRefusal(runErr)
			}
			view := backupView{res: res, hints: []Hint{{
				Cmd: "messq doctor --data-dir " + dataDir,
				Why: "read-only diagnosis runs while the daemon stays up",
			}}}
			out := env.stdoutOrDiscard()
			switch format {
			case render.FormatJSON, render.FormatNDJSON:
				if encErr := encodeOneLine(out, view.JSON()); encErr != nil {
					return fmt.Errorf("write backup document: %w", encErr)
				}
			case render.FormatTable, render.FormatAuto:
				if tableErr := view.Table(out); tableErr != nil {
					return fmt.Errorf("write backup receipt: %w", tableErr)
				}
				if hintErr := WriteHints(out, view.Hints()); hintErr != nil {
					return fmt.Errorf("write backup hints: %w", hintErr)
				}
			}
			return nil
		},
	}
	fs := cmd.Flags()
	fs.String("data-dir", "", "source data directory holding messq.db")
	fs.Bool("force", false, "overwrite an existing destination (still temp+rename, never partial)")
	fs.String("verify", "quick", "post-snapshot self-check: quick|full|none")
	cmd.Annotations = map[string]string{annExits: "0,1,2,4,7"}
	return cmd
}

// encodeOneLine writes one JSON document plus newline — the single-line shape
// both machine modes agree on for scalar results.
func encodeOneLine(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}

// parseVerifyMode maps the flag onto the ops enum; only quick|full|none are
// legal, anything else refuses before Plan runs.
func parseVerifyMode(s string) backup.VerifyMode {
	switch s {
	case "full":
		return backup.VerifyFull
	case "none":
		return backup.VerifyNone
	case "quick":
		return backup.VerifyQuick
	default:
		return backup.VerifyMode(255)
	}
}

// exactlyOneBackupDest refuses missing or surplus positionals with a teaching
// sentence instead of cobra's bare phrasing.
func exactlyOneBackupDest(cmd *cobra.Command, args []string) error {
	switch {
	case len(args) == 0:
		return uierr.Usage("missing destination path: messq backup takes exactly one argument")
	case len(args) > 1:
		return uierr.Usage("unexpected argument %q: messq backup takes exactly one destination", args[1])
	default:
		return nil
	}
}

// teachBackupRefusal turns the ops package's typed refusals into documented
// teaching errors + exit codes. Anything untyped rides back for the funnel,
// which renders it at exit 1 with its own sentence.
func teachBackupRefusal(err error) error {
	var exists *backup.DestinationExistsError
	if errors.As(err, &exists) {
		return &uierr.UserError{
			Code:    "destination_exists",
			Summary: exists.Error(),
			Because: "overwriting an existing snapshot without --force would destroy " +
				"the only good copy if this run fails halfway",
			Next:  []string{"messq backup " + exists.Path + " --force"},
			Exit:  exit.Conflict,
			Cause: err,
		}
	}
	var inside *backup.InsideDataDirError
	if errors.As(err, &inside) {
		return &uierr.UserError{
			Code:    "inside_data_dir",
			Summary: inside.Error(),
			Because: "a snapshot under the data directory joins the NEXT snapshot and " +
				"confuses messq verify",
			Next:  []string{"messq backup --help"},
			Exit:  exit.Usage,
			Cause: err,
		}
	}
	var unwritable *backup.NotWritableError
	if errors.As(err, &unwritable) {
		return &uierr.UserError{
			Code:    "dest_not_writable",
			Summary: unwritable.Error(),
			Because: "the destination directory must accept file creation by the user " +
				"running messq",
			Next:  []string{"chmod u+w " + unwritable.Dir},
			Exit:  exit.Denied,
			Cause: err,
		}
	}
	if errors.Is(err, backup.ErrUsage) {
		msg := strings.TrimPrefix(err.Error(), "usage: ")
		return &uierr.UserError{
			Code:    "usage",
			Summary: msg,
			Because: "VACUUM INTO writes a seekable file; streaming to stdout would be " +
				"a lie about what a backup is",
			Next: []string{"messq backup --help"},
			Exit: exit.Usage,
		}
	}
	return err
}
