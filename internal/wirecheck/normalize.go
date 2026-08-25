// SPDX-License-Identifier: Apache-2.0

package wirecheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// The mask tables. Deliberately narrow (issue #18 §4): everything whose zero value or
// drift carries contract meaning stays visible in goldens, and only genuinely volatile
// values are masked. seq/attempt/ack_token and friends are NEVER masked — the D7 token
// is deterministic in a scripted run, so a golden containing orders/worker/3/2/1 is a
// free fencing-arithmetic assertion.

// NeverMasked is the allow-list of key names that must never join the mask tables.
// TestNeverMaskedDisjointFromMaskRules fails if a future edit adds one anyway.
var NeverMasked = []string{
	"seq", "attempt", "max_deliver", "pending", "backlog",
	"hold_reason", "duplicate", "code", "applied", "ack_token",
}

// maskedNumberKeys are whole-key number masks: any numeric value under one of these
// keys becomes <NUM>. Masking is path-scoped, never value-shaped: a big integer under
// an unmasked key is contract surface and stays.
var maskedNumberKeys = []string{
	"uptime_ms", "db_bytes", "wal_bytes", "disk_free_bytes",
	"stats_age_ms", "age_ms", "created_at", "published_at",
}

// versionedKeys mask build metadata that changes per binary.
var versionedKeys = map[string]string{
	"version": "<VERSION>",
	"commit":  "<COMMIT>",
	"go":      "<GO>",
}

var (
	ulidRe  = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	traceRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
	tsRe    = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`)
)

// Normalizer rewrites volatile values out of a canonical JSON document so goldens stay
// stable across runs while every contract-bearing value survives verbatim.
type Normalizer struct {
	// WorkDir is the daemon's scratch directory for this run; paths under it are
	// rewritten to their operator-facing canonical form (/run/messq/messq.sock),
	// because the document shows the path an operator actually has.
	WorkDir string
}

// NewNormalizer builds a Normalizer for a daemon running in the given work dir.
func NewNormalizer(workDir string) Normalizer { return Normalizer{WorkDir: workDir} }

// Normalize canonicalises raw and applies the mask tables. Idempotence is by
// construction — no rule matches a replacement it emits — and is property-tested
// (TestNormalizeIdempotentProperty) because -update depends on the fixed point.
func (n Normalizer) Normalize(raw []byte) ([]byte, error) {
	canonical, err := CanonBytes(rematerialisePlaceholders(raw))
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(canonical))
	dec.UseNumber()
	var tree any
	if err := dec.Decode(&tree); err != nil {
		return nil, fmt.Errorf("wirecheck: decode: %w", err)
	}
	tree = maskTree(tree, "", n)
	var buf bytes.Buffer
	if err := writeCanonical(&buf, tree, 0); err != nil {
		return nil, err
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// maskTree rewrites node in place and returns it. key is the member name the value was
// found under; array elements inherit their parent member's name.
func maskTree(node any, key string, n Normalizer) any {
	switch v := node.(type) {
	case map[string]any:
		for k, val := range v {
			v[k] = maskTree(val, k, n)
		}
		return v
	case []any:
		for i := range v {
			v[i] = maskTree(v[i], key, n)
		}
		return v
	default:
		return n.maskValue(key, node)
	}
}

// maskValue returns the masked replacement for a scalar found under key, or the value
// itself when nothing matches.
func (n Normalizer) maskValue(key string, val any) any {
	switch s := val.(type) {
	case json.Number:
		if n.numberKeyMasked(key) {
			return Raw(phNum)
		}
	case string:
		if versionedKeys[key] != "" {
			return versionedKeys[key]
		}
		switch {
		case ulidRe.MatchString(s):
			return "<ULID>"
		case traceRe.MatchString(s):
			return "<TRACE>"
		case tsRe.MatchString(s):
			return "<TS>"
		default:
			return n.rewritePaths(s)
		}
	}
	return val
}

func (n Normalizer) rewritePaths(s string) string {
	if n.WorkDir == "" {
		return s
	}
	return strings.ReplaceAll(s, n.WorkDir+"/messq.sock", "/run/messq/messq.sock")
}

func (Normalizer) numberKeyMasked(key string) bool {
	for _, k := range maskedNumberKeys {
		if k == key {
			return true
		}
	}
	return atSuffixMask.MatchString(key) || ageSuffixMask.MatchString(key)
}

// phNum is the numeric placeholder the mask pass emits.
const phNum = "<NUM>"

var (
	atSuffixMask  = regexp.MustCompile(`_at$`)
	ageSuffixMask = regexp.MustCompile(`_age_ms$`)
	// arrayPlaceholderLine matches a canonical array element that is nothing but a
	// placeholder: the only other shape writeCanonical emits one in.
	arrayPlaceholderLine = regexp.MustCompile(`(?m)^\s+<NUM>$`)
)

// rematerialisePlaceholders turns previously emitted bare <NUM> tokens back into
// plain numbers so the document parses again; the mask pass below immediately
// re-masks them (the key under each still matches), which is what makes Normalize
// idempotent on its own output.
func rematerialisePlaceholders(b []byte) []byte {
	b = bytes.ReplaceAll(b, []byte(": "+phNum), []byte(": 0"))
	return arrayPlaceholderLine.ReplaceAll(b, []byte(" 0"))
}
