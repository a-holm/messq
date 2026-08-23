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
#
# Every rule names a scope. `prod` walks the production build only. `test` also walks what the
# test binary links in, because `go list -deps` on its own cannot see a _test.go file: a test in
# package queue that imports internal/store is exactly the violation these rules exist to stop,
# and it would otherwise be invisible. pkg/client is the one rule that stays production-only:
# its tests legitimately drive an in-process server and reuse the helpers in internal/testutil,
# and the promise there is about the published API surface, not about the test binary.
set -euo pipefail

module="$(go list -m)"
status=0

# Every rule below reads an import graph, and an import graph computed from a tree that does not
# load is not evidence of anything. Load the whole module once, with -e so the failures arrive
# as data rather than as an exit status, and refuse to judge anything if a package failed.
# Without this a directory whose files declare two different package names yields an empty
# dependency list, every rule finds nothing forbidden in it, and the script reports that the
# directions hold.
#
# A syntax error inside a function body is deliberately not covered here: go list parses package
# clauses and imports, not bodies, so the import graph it returns is still correct. `make vet`
# is the gate for that, and it has a sabotage row of its own.
load_errors="$(
	go list -e -deps -test \
		-f '{{with .Error}}{{$.ImportPath}}: {{.Err}}{{end}}{{range .DepsErrors}}{{$.ImportPath}}: {{.Err}}{{end}}' \
		./... 2>&1 | grep -v '^$' | grep -v '^go: downloading ' || true
)"
if [[ -n "$load_errors" ]]; then
	echo "layers: the tree does not load, so its import graph proves nothing:" >&2
	echo "$load_errors" >&2
	exit 1
fi

# deps_of <scope> <package> lists every package reachable from <package>. Under the test scope,
# go list appends the test binary's own variants in brackets; the suffix is stripped so the
# import paths compare as plain lines.
deps_of() {
	local scope="$1" pkg="$2"
	if [[ "$scope" == test ]]; then
		go list -deps -test "$module/$pkg" | sed 's/ \[.*\]$//' | sort -u
	else
		go list -deps "$module/$pkg"
	fi
}

# imports_of <scope> <package> lists the imports written in <package>'s own files.
imports_of() {
	local scope="$1" pkg="$2"
	if [[ "$scope" == test ]]; then
		go list -f '{{join .Imports "\n"}}{{"\n"}}{{join .TestImports "\n"}}{{"\n"}}{{join .XTestImports "\n"}}' "$module/$pkg"
	else
		go list -f '{{join .Imports "\n"}}' "$module/$pkg"
	fi
}

# scope_suffix names the scope in a failure message, so a violation that only the test binary
# introduces does not read as a production import.
scope_suffix() {
	if [[ "$1" == test ]]; then
		echo " or its tests"
	fi
}

# forbid_deps <scope> <package> <reason> <import>... fails when <package> reaches <import> at all.
forbid_deps() {
	local scope="$1" pkg="$2" reason="$3"
	shift 3
	local deps bad
	deps="$(deps_of "$scope" "$pkg")"
	for bad in "$@"; do
		if grep -Fxq -- "$bad" <<<"$deps"; then
			echo "layers: $pkg$(scope_suffix "$scope") depends on $bad, directly or transitively. $reason" >&2
			status=1
		fi
	done
}

# forbid_imports <scope> <package> <reason> <import>... fails only on an import in <package>'s own files.
forbid_imports() {
	local scope="$1" pkg="$2" reason="$3"
	shift 3
	local imports bad
	imports="$(imports_of "$scope" "$pkg")"
	for bad in "$@"; do
		if grep -Fxq -- "$bad" <<<"$imports"; then
			echo "layers: $pkg$(scope_suffix "$scope") imports $bad in its own files. $reason" >&2
			status=1
		fi
	done
}

queue_reason="PLAN.md section 3.3: internal/queue is a pure state machine with no I/O."
forbid_deps test internal/queue "$queue_reason" \
	database/sql net/http \
	"$module/internal/store" "$module/internal/api" "$module/internal/cli" "$module/internal/obs"
forbid_imports test internal/queue "$queue_reason" \
	os os/exec os/signal os/user syscall net

forbid_deps test internal/store \
	"Issue #1 design: internal/store sits below the API and CLI layers and must not reach up." \
	net/http "$module/internal/api" "$module/internal/cli"

forbid_deps test internal/model \
	"Issue #1 design: internal/model is an independent oracle and must not share code with what it checks." \
	"$module/internal/queue" "$module/internal/store"

# The four primitives of issue #3 sit below everything else. They are checked transitively
# rather than by direct import: a leaf that reaches the store through one hop is still not a
# leaf. internal/errs is the bottom of the tree and internal/clock is the seam, so those two
# are the only internal packages the others may reach.
leaf_reason="Issue #3 design: this package is a leaf below internal/queue. Only internal/clock and internal/errs may be reached from it."
leaf_allowed="$module/internal/clock
$module/internal/errs"

check_leaf() {
	local pkg="$1" dep self="$module/$1"
	while read -r dep; do
		if [[ -z "$dep" || "$dep" == "$self" || "$dep" == "${self}_test" || "$dep" == "${self}.test" ]]; then
			continue
		fi
		if ! grep -Fxq -- "$dep" <<<"$leaf_allowed"; then
			echo "layers: $pkg or its tests depends on $dep, directly or transitively. $leaf_reason" >&2
			status=1
		fi
	done < <(deps_of test "$pkg" | grep "^$module/internal/" || true)
}

for leaf in internal/subject internal/id internal/clock internal/errs; do
	check_leaf "$leaf"
done

client_reason="Issue #1 design: pkg/client is public and must not depend on internal packages."
# Two steps, for the reason dep-budget.sh gives: grep exits 1 when nothing leaked, which is the
# expected state, so its status must be tolerated. Tolerating deps_of's status in the same
# pipeline would report that pkg/client is clean whenever go list could not list it.
client_deps="$(deps_of prod pkg/client)"
leaked="$(grep "^$module/internal/" <<<"$client_deps" || true)"
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
