// SPDX-License-Identifier: Apache-2.0

package clitest_test

import (
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli"
	"github.com/a-holm/messq/internal/cli/clitest"
	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/cli/uierr"
	"github.com/spf13/cobra"
)

// TestEveryExitCodeIsProduced drives one scenario per documented code 0–7 and fails
// on any exit code the contract documents but the CLI can never return. Daemon-side
// scenarios ride version --remote against scripted fake-daemon envelopes; the
// locally-produced codes go through the same funnel via a scratch command.
func TestEveryExitCodeIsProduced(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		route   *clitest.Response
		noRoute bool // scenario never touches a daemon
		local   bool // usage: must not dial at all
	}{
		{
			name: "0 ok — version --remote answers",
			code: exit.OK,
			route: &clitest.Response{
				Status: 200,
				Body:   `{"version":"v9.9.9-test","uptime_ms":1,"durability":"full","synchronous":2,"db_bytes":10,"node_id":"n1"}`,
			},
		},
		{
			name:  "1 internal",
			code:  exit.Error,
			route: &clitest.Response{Status: 500, Body: `{"error":{"code":"internal","message":"boom"}}`},
		},
		{
			name:  "1 unknown future code degrades to error, never usage",
			code:  exit.Error,
			route: &clitest.Response{Status: 599, Body: `{"error":{"code":"holographic_storage_required","message":"from the future"}}`},
		},
		{
			name:  "2 usage stays local — no daemon involved",
			code:  exit.Usage,
			local: true,
		},
		{
			name:  "3 not_found",
			code:  exit.NotFound,
			route: &clitest.Response{Status: 404, Body: `{"error":{"code":"not_found","message":"no such thing"}}`},
		},
		{
			name:  "4 conflict family (stream_exists)",
			code:  exit.Conflict,
			route: &clitest.Response{Status: 409, Body: `{"error":{"code":"stream_exists","message":"orders is already there"}}`},
		},
		{
			name:    "5 wait expired is produced locally through the funnel",
			code:    exit.Empty,
			noRoute: true,
		},
		{
			name:    "6 unreachable — no daemon at the socket",
			code:    exit.Unreachable,
			noRoute: true,
		},
		{
			name:  "7 forbidden",
			code:  exit.Denied,
			route: &clitest.Response{Status: 403, Body: `{"error":{"code":"forbidden","message":"not your token"}}`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := clitest.NewFakeDaemon(t)
			if tt.route != nil {
				d.Route("GET", "/v1/info", *tt.route)
			}

			var args []string
			var runner clitest.Runner
			switch {
			case tt.noRoute && tt.code == exit.Unreachable:
				// A socket that cannot exist: transport failure → 6.
				args = []string{"--addr", "unix:///nonexistent/messq-missing.sock", "version", "--remote"}
			case tt.noRoute && tt.code == exit.Empty:
				runner = clitest.Runner{Build: waitExpiredTree}
				args = []string{"--output", "json", "waitprobe"}
			case tt.code == exit.Usage:
				args = []string{"version", "--output", "yaml"}
				defer func() {
					if n := len(d.Requests()); n != 0 && t.Failed() {
						t.Logf("usage failure dialled the daemon %d time(s)", n)
					}
				}()
			default:
				args = []string{"--addr", d.Addr(), "version", "--remote"}
			}

			res := clitest.Run(t, runner, args...)
			if res.Exit != tt.code {
				t.Fatalf("exit = %d, want %d (stderr %.160q)", res.Exit, tt.code, res.Stderr)
			}
		})
	}
}

// TestVersionRemoteIsStrictlyOptIn pins the Dockerfile contract: `messq version`
// must never perform network I/O, so it can never hang waiting for a daemon.
func TestVersionRemoteIsStrictlyOptIn(t *testing.T) {
	d := clitest.NewFakeDaemon(t)
	d.Route("GET", "/v1/info", clitest.Response{
		Status: 200,
		Body:   `{"version":"v9.9.9-test","uptime_ms":1}`,
	})
	res := clitest.Run(t, clitest.Runner{
		Env: map[string]string{"MESSQ_ADDR": d.Addr()},
	}, "version", "--output", "table")
	if res.Exit != 0 {
		t.Fatalf("exit = %d (stderr %.160q)", res.Exit, res.Stderr)
	}
	if n := len(d.Requests()); n != 0 {
		t.Fatalf("offline version performed %d request(s); --remote must be opt-in", n)
	}
	if strings.Contains(res.Stdout, "server") {
		t.Errorf("offline output contains remote data: %q", res.Stdout)
	}

	remote := clitest.Run(t, clitest.Runner{
		Env: map[string]string{"MESSQ_ADDR": d.Addr()},
	}, "version", "--remote", "--output", "table")
	if remote.Exit != 0 {
		t.Fatalf("--remote exit = %d (stderr %.160q)", remote.Exit, remote.Stderr)
	}
	if n := len(d.Requests()); n != 1 {
		t.Fatalf("--remote made %d request(s), want exactly 1", n)
	}
	if !strings.Contains(remote.Stdout, "v9.9.9-test") {
		t.Errorf("--remote did not surface the server build: %q", remote.Stdout)
	}
}

func waitExpiredTree(env *cli.Env) *cobra.Command {
	root := cli.NewRoot(env)
	scratch := &cobra.Command{
		Use:   "waitprobe",
		Short: "probe a long poll that returned nothing",
		Long: "A scratch command used by the exit-matrix test to prove the locally-produced " +
			"empty/wait-expired outcome reaches the process exit intact through the same " +
			"funnel every real command uses.",
		Example: "  messq waitprobe",
		Args:    cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return &uierr.UserError{
				Code:    "wait_expired",
				Summary: "the long poll returned nothing before its deadline",
				Next:    []string{"messq sub orders billing --count 3"},
				Exit:    exit.Empty,
			}
		},
	}
	scratch.Annotations = map[string]string{"messq.exits": "5"}
	root.AddCommand(scratch)
	return root
}
