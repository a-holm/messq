// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"context"
	"fmt"

	"github.com/a-holm/messq/internal/buildinfo"
)

func init() {
	defaultRegistry.Register(Check{
		ID:      "server.unreachable",
		Summary: "the daemon this run pointed at did not answer",
		Explain: "Doctor diagnoses a running daemon over --addr. When nothing answers, " +
			"every live-only check degrades to skip and THIS finding is the one that " +
			"matters: either the daemon is down (start it) or --addr points somewhere " +
			"else than the daemon listens on.",
		Needs: SourceLive,
		Eval:  evalServerUnreachable,
	})
	defaultRegistry.Register(Check{
		ID:      "server.version_skew",
		Summary: "CLI version differs from the daemon's version",
		Explain: "A skew between the driving binary and the serving binary means the " +
			"two disagree about wire details by accident of build date. Upgrade " +
			"whichever is behind; doctor reports rather than guesses which.",
		Needs: SourceLive,
		Eval:  evalServerVersionSkew,
	})
}

// evalServerUnreachable fires only in live mode when collection recorded an
// unreachable daemon — offline runs have no server to miss.
func evalServerUnreachable(_ context.Context, snap *Snapshot) []Finding {
	if snap.Source != SourceLive || snap.Unreachable == "" {
		return nil
	}
	return []Finding{{
		ID:       "server.unreachable",
		Severity: SevFail,
		Title:    "no daemon answered at the address this run used",
		Detail: fmt.Sprintf("%s\nEvery check that needs a running daemon reported "+
			"[skip]; only offline sources were diagnosed.", renderSafe(snap.Unreachable)),
		Fix: []string{
			"start the messq daemon you meant to diagnose",
			"messq doctor --addr <addr>   # rerun against the right socket",
		},
		Docs: docsAnchor("server.unreachable"),
	}}
}

// evalServerVersionSkew compares the daemon's build with the CLI's own once
// both are known; development builds without version stamps report skipped
// rather than guessing.
func evalServerVersionSkew(_ context.Context, snap *Snapshot) []Finding {
	if snap.Source != SourceLive || snap.Server == nil {
		return []Finding{{
			ID:       "server.version_skew",
			Severity: SevSkipped,
			Title:    "needs a running daemon (try --addr)",
			NoFix:    "this is informational; nothing was diagnosed offline here",
			Docs:     docsAnchor("server.version_skew"),
		}}
	}
	daemon := snap.Server.Version
	local := buildinfo.Get().Version
	if daemon == "" || local == "" || daemon == local {
		return nil // equal versions: healthy; unstampable builds: not doctor's lie to tell
	}
	return []Finding{{
		ID:       "server.version_skew",
		Severity: SevInfo,
		Title:    fmt.Sprintf("daemon %s but CLI %s", daemon, local),
		Detail:   "The two binaries may disagree on wire details that shipped between builds.",
		Fix:      []string{"upgrade whichever of the two is older"},
		Evidence: map[string]any{"daemon_version": daemon, "cli_version": local},
		Docs:     docsAnchor("server.version_skew"),
	}}
}
