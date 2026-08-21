#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Guards the dependency directions between packages.
#
# The internal/queue rules come from PLAN.md section 3.3, which makes the state machine a pure
# function with no I/O. The internal/store, internal/model and pkg/client rules come from the
# design table in issue #1 and have no PLAN.md section of their own; each message cites its
# real source.
#
# Two kinds of check, because they are not interchangeable. A transitive check (go list -deps)
# is right for a package that must not be reachable at all. A direct-import check
# (go list .Imports) is right for os and its neighbours, because much of the standard library
# reaches os transitively: even `import "fmt"` pulls it in, so a transitive os rule would
# forbid printing.
#
# The direct-import rules see one hop only. A new internal package that imports os and is then
# imported by internal/queue passes this script; catching that needs a transitive check with an
# allow-list of intermediate packages, which is not worth building while there is one internal
# package below queue. Code review covers the gap until then.
set -euo pipefail

module="$(go list -m)"
status=0

# forbid_deps <package> <reason> <import>... fails when <package> reaches <import> at all.
forbid_deps() {
	local pkg="$1" reason="$2"
	shift 2
	local deps bad
	deps="$(go list -deps "$module/$pkg")"
	for bad in "$@"; do
		if grep -Fxq -- "$bad" <<<"$deps"; then
			echo "layers: $pkg depends on $bad, directly or transitively. $reason" >&2
			status=1
		fi
	done
}

# forbid_imports <package> <reason> <import>... fails only on an import in <package>'s own files.
forbid_imports() {
	local pkg="$1" reason="$2"
	shift 2
	local imports bad
	imports="$(go list -f '{{join .Imports "\n"}}' "$module/$pkg")"
	for bad in "$@"; do
		if grep -Fxq -- "$bad" <<<"$imports"; then
			echo "layers: $pkg imports $bad in its own files. $reason" >&2
			status=1
		fi
	done
}

queue_reason="PLAN.md section 3.3: internal/queue is a pure state machine with no I/O."
forbid_deps internal/queue "$queue_reason" \
	database/sql net/http \
	"$module/internal/store" "$module/internal/api" "$module/internal/cli" "$module/internal/obs"
forbid_imports internal/queue "$queue_reason" \
	os os/exec os/signal os/user syscall net

forbid_deps internal/store \
	"Issue #1 design: internal/store sits below the API and CLI layers and must not reach up." \
	net/http "$module/internal/api" "$module/internal/cli"

forbid_deps internal/model \
	"Issue #1 design: internal/model is an independent oracle and must not share code with what it checks." \
	"$module/internal/queue" "$module/internal/store"

client_reason="Issue #1 design: pkg/client is public and must not depend on internal packages."
leaked="$(go list -deps "$module/pkg/client" | grep "^$module/internal/" || true)"
if [[ -n "$leaked" ]]; then
	while read -r dep; do
		echo "layers: pkg/client depends on $dep, directly or transitively. $client_reason" >&2
	done <<<"$leaked"
	status=1
fi

if ((status == 0)); then
	echo "layers: dependency directions hold"
fi

exit "$status"
