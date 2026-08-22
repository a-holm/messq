#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Enforces the dependency budget of PLAN.md section 13. The budget is an allow-list rather than
# a count, so a transitive module promoted to a direct require fails loudly instead of quietly
# spending the budget.
#
# The list holds ten module paths for the eight budget rows of PLAN.md section 13, which writes
# the CLI row as "spf13/cobra (+pflag)": pflag is part of that one entry, and the storage row
# names both engines — modernc.org/sqlite plus mattn/go-sqlite3 as "a tested build-tag escape
# hatch" (row 1, D1), which is exactly how driver_cgo.go wires it.
set -euo pipefail

allowed=(
	github.com/google/go-cmp
	github.com/mattn/go-sqlite3
	github.com/oklog/ulid/v2
	github.com/prometheus/client_golang
	github.com/rogpeppe/go-internal
	github.com/spf13/cobra
	github.com/spf13/pflag
	golang.org/x/sys
	modernc.org/sqlite
	pgregory.net/rapid
)

is_allowed() {
	local mod="$1" a
	for a in "${allowed[@]}"; do
		if [[ "$mod" == "$a" ]]; then
			return 0
		fi
	done
	return 1
}

# The two steps are separate on purpose. `grep -v '^$'` exits 1 when every line it saw was
# blank, which is the legitimate state of a module with no dependencies, so its status has to be
# tolerated. Tolerating go list's status in the same pipeline would turn an unreadable module
# graph into "0 direct modules, all allow-listed": a budget that passes precisely because it
# could not be measured.
if ! listing="$(go list -m -f '{{if and (not .Main) (not .Indirect)}}{{.Path}}{{end}}' all)"; then
	echo "dep-budget: go list -m all failed, so the module graph is unreadable and the budget is unknown" >&2
	exit 1
fi

mapfile -t direct < <(grep -v '^$' <<<"$listing" || true)

status=0
for mod in ${direct[@]+"${direct[@]}"}; do
	if ! is_allowed "$mod"; then
		echo "dep-budget: new direct dependency $mod: add an ADR under docs/adr/ and extend scripts/dep-budget.sh" >&2
		status=1
	fi
done

if ((status == 0)); then
	echo "dep-budget: ${#direct[@]}/${#allowed[@]} direct modules, all allow-listed"
fi

exit "$status"
