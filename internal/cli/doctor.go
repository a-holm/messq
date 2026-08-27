// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/flagx"
	"github.com/a-holm/messq/internal/cli/render"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/a-holm/messq/internal/doctor"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// pflag aliases keep sliceOf's signature readable without importing
// everything under two spellings.
type (
	pflagFlag  = pflag.Flag
	pflagSlice = pflag.SliceValue
)

// flagValue layers one setting like conf does (flag > env > default) but with
// the issue's MESSQ_DOCTOR_* variable name, which conf's mechanical derivation
// cannot produce. A Changed flag wins; else the named env; else the default.
func doctorFlag(cmd *cobra.Command, getenv func(string) string, flag, messqEnv string) string {
	f := cmd.Flags().Lookup(flag)
	if f != nil && f.Changed {
		return f.Value.String()
	}
	if getenv != nil {
		if v := getenv(messqEnv); v != "" {
			return v
		}
	}
	if f == nil {
		return ""
	}
	return f.DefValue
}

// parseDoctorDuration reads a duration setting through flagx's grammar
// (seconds/days included) so cron lines can spell 30d instead of 720h.
func parseDoctorDuration(raw, what string) (time.Duration, error) {
	var d flagx.Duration
	if err := d.Set(raw); err != nil {
		return 0, uierr.Usage("invalid %s %q: %v", what, raw, err)
	}
	return time.Duration(d), nil
}

// newDoctorCmd wires `messq doctor` onto the chassis: source selection by
// --data-dir/--addr, the §9 flag set, --list/--explain as documentation, and
// the §10 exit contract where an unreachable daemon is a finding and never a
// transport exit.
func newDoctorCmd(env *Env) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "diagnose configuration and operations in prose",
		Long: "Run opinionated health checks over a running daemon (--addr) or an " +
			"offline data directory (--data-dir) and print prose findings with exact " +
			"fix commands.\n" +
			"\n" +
			"Checks are pure functions over one collected snapshot; anything they " +
			"cannot see is reported as [skip] with a reason, never silently omitted. " +
			"An unreachable daemon is itself a fail finding — doctor is designed to " +
			"run when things are already broken, so it never exits 6.",
		Example: "  messq doctor\n" +
			"  messq doctor --data-dir /var/lib/messq --fail-on warn --quiet\n" +
			"  messq doctor --list\n" +
			"  messq doctor --explain storage.wal_size",
		GroupID: "operate",
		Args:    exactArgsMessage,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags := cmd.Flags()

			// Documentation modes answer without touching any source.
			explainID, xErr := flags.GetString("explain")
			if xErr != nil {
				return uierr.Usage("%s", xErr)
			}
			if explainID != "" {
				return runDoctorExplain(cmd, env, explainID)
			}
			listOnly, lErr := flags.GetBool("list")
			if lErr != nil {
				return uierr.Usage("%s", lErr)
			}
			if listOnly {
				return runDoctorList(cmd, env)
			}

			failOn := doctorFlag(cmd, env.Getenv, "fail-on", "MESSQ_DOCTOR_FAIL_ON")
			if _, pErr := doctor.ParseFailOn(failOn); pErr != nil {
				return uierr.Usage("%s", pErr)
			}

			dur := func(flag, messqEnv, label string) time.Duration {
				raw := doctorFlag(cmd, env.Getenv, flag, messqEnv)
				d, dErr := parseDoctorDuration(raw, label)
				if dErr != nil {
					return -1 // sentinel: reported by the caller below
				}
				return d
			}
			var badUsage error
			sinceDur := dur("since", "MESSQ_DOCTOR_SINCE", "--since")
			if sinceDur < 0 {
				badUsage = uierr.Usage("invalid --since %q",
					doctorFlag(cmd, env.Getenv, "since", "MESSQ_DOCTOR_SINCE"))
			}
			idleDur := dur("idle-after", "MESSQ_DOCTOR_IDLE_AFTER", "--idle-after")
			if idleDur < 0 {
				badUsage = uierr.Usage("invalid --idle-after %q",
					doctorFlag(cmd, env.Getenv, "idle-after", "MESSQ_DOCTOR_IDLE_AFTER"))
			}
			maxAgeDur := dur("backup-max-age", "MESSQ_BACKUP_MAX_AGE", "--backup-max-age")
			if maxAgeDur < 0 {
				badUsage = uierr.Usage("invalid --backup-max-age %q",
					doctorFlag(cmd, env.Getenv, "backup-max-age", "MESSQ_BACKUP_MAX_AGE"))
			}
			if badUsage != nil {
				return badUsage
			}
			opts := doctor.RunOptions{
				Addr:         cmd.Flags().Lookup("addr").Value.String(),
				DataDir:      doctorFlag(cmd, env.Getenv, "data-dir", "MESSQ_DATA_DIR"),
				Clock:        seamClock{now: env.Now},
				Since:        sinceDur,
				IdleAfter:    idleDur,
				BackupDir:    doctorFlag(cmd, env.Getenv, "backup-dir", "MESSQ_BACKUP_DIR"),
				BackupMaxAge: maxAgeDur,
			}

			format := render.FormatTable
			colour := false
			quiet := flags.Lookup("quiet").Value.String() == "true"
			if s := sessionFrom(cmd); s != nil {
				format = s.format
				colour = s.colour
			}

			runCtx, cancel := context.WithCancel(cmd.Context())
			defer cancel()
			snap, cErr := doctor.Collect(runCtx, opts)
			if cErr != nil {
				return teachDoctorSourceRefusal(cErr)
			}

			reg := doctor.DefaultRegistry()
			reg, unknown := doctor.FilterRegistry(reg,
				sliceOf(flags.Lookup("only")), sliceOf(flags.Lookup("skip")))
			if len(unknown) > 0 {
				return uierr.Usage("unknown check id(s) in --only/--skip: %s "+
					"(run 'messq doctor --list' for the set)", strings.Join(unknown, ", "))
			}

			findings := doctor.RunChecks(runCtx, reg, snap)
			rep := doctor.Report{
				GeneratedAt: env.Now(),
				Source:      snap.Source,
				Target: doctor.Target{
					Addr:    opts.Addr,
					DataDir: opts.DataDir,
					Version: versionOfSnapshot(snap),
				},
				Findings: findings,
				Checks:   len(reg.List()),
			}
			out := env.stdoutOrDiscard()
			switch format {
			case render.FormatJSON:
				if encErr := encodeOneLine(out, doctor.JSONDocument(rep)); encErr != nil {
					return fmt.Errorf("write doctor json: %w", encErr)
				}
			case render.FormatNDJSON:
				if ndErr := writeNDJSONReport(out, rep); ndErr != nil {
					return fmt.Errorf("write doctor ndjson: %w", ndErr)
				}
			case render.FormatTable, render.FormatAuto:
				hErr := doctor.WriteHuman(out, rep, doctor.HumanOpts{Quiet: quiet, Colour: colour})
				if hErr != nil {
					return fmt.Errorf("write doctor face: %w", hErr)
				}
			default:
				return uierr.Usage("unhandled output mode")
			}

			code := doctor.Summarize(findings, rep.Checks, rep.Duration).ExitCodeFor(failOn)
			if code != exit.OK {
				// The faces above are the deliverable; the funnel must honour
				// the documented code without rendering a second failure.
				return &silentExit{code: code}
			}
			return nil
		},
	}
	fs := cmd.Flags()
	fs.String("data-dir", "", "offline source: diagnose this data directory read-only")
	fs.String("since", "1h", "analysis window for event- and metric-derived checks")
	fs.String("idle-after", "24h", "threshold for the consumer.idle* checks")
	fs.String("backup-dir", "", "enables the backup.* checks against this directory")
	fs.String("backup-max-age", "168h", "age at which backup.stale fires")
	fs.String("fail-on", "warn", "what makes the exit code 1: info|warn|fail|never")
	fs.StringSlice("only", nil, "comma-separated check ids or family globs to include")
	fs.StringSlice("skip", nil, "comma-separated check ids or family globs to exclude")
	fs.Bool("list", false, "print the registered checks and their summaries")
	fs.String("explain", "", "print the teaching paragraph for one check id")
	cmd.Annotations = map[string]string{annExits: "0,1,2,7"}
	return cmd
}

// teachDoctorSourceRefusal maps collection refusals onto §10's exit 7: neither
// source was readable, so there is nothing honest to diagnose or print.
func teachDoctorSourceRefusal(err error) error {
	return &uierr.UserError{
		Code:    "no_source_readable",
		Summary: err.Error(),
		Because: "doctor needs either a reachable --addr or a readable --data-dir; " +
			"both failed, so it refuses to invent findings",
		Next:  []string{"messq doctor --data-dir /var/lib/messq"},
		Exit:  exit.Denied,
		Cause: err,
	}
}

func versionOfSnapshot(snap *doctor.Snapshot) string {
	if snap.Server != nil {
		return snap.Server.Version
	}
	return ""
}

// sliceOf flattens a StringSlice flag into its values ([]string typed).
func sliceOf(f *pflagFlag) []string {
	if f == nil || !f.Changed {
		return nil
	}
	var out []string
	switch v := f.Value.(type) {
	case pflagSlice:
		for _, item := range v.GetSlice() {
			if strings.TrimSpace(item) != "" {
				out = append(out, strings.TrimSpace(item))
			}
		}
	default:
		if f.Value.String() != "" {
			out = append(out, f.Value.String())
		}
	}
	return out
}

func runDoctorExplain(cmd *cobra.Command, env *Env, id string) error {
	reg := doctor.DefaultRegistry()
	explain, ok := reg.Explain(id)
	if !ok {
		return uierr.Usage("unknown check id %q: run 'messq doctor --list'", id)
	}
	fmt.Fprintf(env.stdoutOrDiscard(), "%s\n\n%s\n", render.Safe(id), renderSafeText(explain))
	return nil
}

func runDoctorList(cmd *cobra.Command, env *Env) error {
	type entry struct {
		ID      string `json:"id"`
		Summary string `json:"summary"`
	}
	reg := doctor.DefaultRegistry()
	var rows []entry
	for _, id := range reg.List() {
		check, ok := reg.Get(id)
		if !ok {
			continue // unreachable: Get over List's own ids
		}
		rows = append(rows, entry{ID: id, Summary: check.Summary})
	}
	format := render.FormatTable
	if s := sessionFrom(cmd); s != nil {
		format = s.format
	}
	out := env.stdoutOrDiscard()
	switch format {
	case render.FormatJSON, render.FormatNDJSON:
		if err := encodeOneLine(out, map[string]any{"checks": rows}); err != nil {
			return fmt.Errorf("write doctor list json: %w", err)
		}
	case render.FormatTable, render.FormatAuto:
		tw := render.NewTableWriter(out)
		sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
		for _, r := range rows {
			if err := tw.WriteLine(r.ID + "\t" + render.Safe(r.Summary)); err != nil {
				return err
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// writeNDJSONReport streams one finding per line and closes with the summary
// object — the summary rides last because it is derived state, not evidence.
func writeNDJSONReport(w io.Writer, rep doctor.Report) error {
	enc := json.NewEncoder(w)
	for _, rec := range doctor.NDJSONRecords(rep) {
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}
	sum := doctor.Summarize(rep.Findings, rep.Checks, rep.Duration)
	return enc.Encode(map[string]any{"summary": sum})
}

func renderSafeText(s string) string { return render.Safe(s) }
