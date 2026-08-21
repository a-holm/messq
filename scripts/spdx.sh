#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Enforces the licence header CONTRIBUTING.md promises. Without this, deleting a header leaves
# `make ci` green and the rule is folklore.
#
# Go files must carry the identifier on their very first line, which is what CONTRIBUTING.md
# states and what keeps the header above the package clause and its doc comment. Everything
# else may carry it under a shebang or a name line, so those get three lines of slack.
#
# The file list comes from find rather than from `git ls-files`: the sabotage matrix runs this
# script against a scratch copy of the tree that is not a git repository. testdata is pruned
# because the fixtures in it are inputs, not source.
set -euo pipefail

cd "$(dirname "$0")/.."

readonly spdx='SPDX-License-Identifier: Apache-2.0'
status=0

# check <max-lines> <file>... fails when the identifier is absent from the first <max-lines>.
check() {
	local limit="$1"
	shift
	local path
	for path in "$@"; do
		if [[ ! -f "$path" ]]; then
			continue
		fi
		if ! head -n "$limit" -- "$path" | grep -qF -- "$spdx"; then
			echo "spdx: $path: missing '$spdx' within the first $limit line(s)" >&2
			status=1
		fi
	done
}

prune=(-name .git -o -name dist -o -name testdata -o -name vendor)

mapfile -t go_files < <(find . \( "${prune[@]}" \) -prune -o -type f -name '*.go' -print | sort)
mapfile -t other_files < <(
	find . \( "${prune[@]}" \) -prune -o -type f \
		\( -name '*.sh' -o -name 'Makefile' -o -path './.github/workflows/*' -o -path './.githooks/*' \) \
		-print | sort
)

check 1 ${go_files[@]+"${go_files[@]}"}
check 3 ${other_files[@]+"${other_files[@]}"} .golangci.yml

if ((status == 0)); then
	echo "spdx: ${#go_files[@]} Go files and ${#other_files[@]} other files carry the licence header"
fi

exit "$status"
