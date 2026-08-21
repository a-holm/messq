#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Asserts a built binary is static, trimmed and cgo-free. ldd is not usable as the oracle: it
# exits 1 on a static binary and cannot inspect a foreign architecture at all, so the
# toolchain's own build record is the source of truth.
set -euo pipefail

if [[ $# -ne 1 ]]; then
	echo "usage: $0 <binary>" >&2
	exit 2
fi
bin="$1"

if [[ ! -f "$bin" ]]; then
	echo "assert-static: $bin does not exist" >&2
	exit 1
fi

record="$(go version -m "$bin")"

setting() {
	awk -F'\t' -v key="$1" \
		'$2 == "build" && index($3, key "=") == 1 { print substr($3, length(key) + 2) }' <<<"$record"
}

require() {
	local key="$1" want="$2" got
	got="$(setting "$key")"
	if [[ "$got" != "$want" ]]; then
		echo "assert-static: $bin: build setting $key is \"${got:-<unset>}\", want \"$want\"" >&2
		exit 1
	fi
}

require CGO_ENABLED 0
require -trimpath true

if ! command -v file >/dev/null; then
	echo "assert-static: file(1) is not installed" >&2
	exit 1
fi
if ! file -- "$bin" | grep -q 'statically linked'; then
	echo "assert-static: $bin: file(1) does not report a statically linked executable: $(file -b -- "$bin")" >&2
	exit 1
fi

echo "assert-static: $bin OK ($(setting GOOS)/$(setting GOARCH), static, trimpath, cgo off)"
