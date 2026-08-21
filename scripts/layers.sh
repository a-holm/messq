#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Guards the dependency directions PLAN.md section 3.3 calls load-bearing: internal/queue is a
# pure state machine, internal/model is an independent oracle, and pkg/client is a public
# package that must not reach into internal/. Checks are transitive (go list -deps).
set -euo pipefail

module="$(go list -m)"
status=0

# forbid <package> <import>... fails when <package> transitively imports any <import>.
forbid() {
	local pkg="$1"
	shift
	local deps bad
	deps="$(go list -deps "$module/$pkg")"
	for bad in "$@"; do
		if grep -Fxq -- "$bad" <<<"$deps"; then
			echo "layers: $pkg must not import $bad (PLAN.md section 3.3)" >&2
			status=1
		fi
	done
}

forbid internal/queue database/sql net/http os \
	"$module/internal/store" "$module/internal/api" "$module/internal/cli" "$module/internal/obs"

forbid internal/store net/http \
	"$module/internal/api" "$module/internal/cli"

forbid internal/model "$module/internal/queue" "$module/internal/store"

leaked="$(go list -deps "$module/pkg/client" | grep "^$module/internal/" || true)"
if [[ -n "$leaked" ]]; then
	while read -r dep; do
		echo "layers: pkg/client must not import $dep (PLAN.md section 3.3)" >&2
	done <<<"$leaked"
	status=1
fi

if ((status == 0)); then
	echo "layers: dependency directions hold"
fi

exit "$status"
