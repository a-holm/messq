// SPDX-License-Identifier: Apache-2.0

// Package wirecheck is the shared machinery behind messq's wire contract (issue #18):
// canonical JSON, the golden normaliser, shape digests and the ADDITIVE/BREAKING change
// classifier. test/contract, test/docs and the later cross-version replay (#34/#36)
// build on it instead of growing private copies.
//
// Layering rule (scripts/layers.sh enforces it): wirecheck reflects over the types it
// is handed and imports no internal package — the API layer passes its wire types in,
// never the reverse.
package wirecheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// Raw is a pre-escaped literal written into canonical output byte-for-byte. The
// normaliser uses it for numeric placeholders so `"created_at": <NUM>` stays a JSON
// number positionally — a shell worker piping a golden through jq still sees a number
// shape there, not a string.
type Raw string

// Canonical renders v as the canonical JSON form: object keys sorted, two-space
// indent, HTML escaping off, numbers preserved as literals (a large int64 never
// float-reformats). It is the form every golden and every doc block is written in.
func Canonical(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return CanonBytes(raw)
}

// CanonBytes canonicalises an already-encoded JSON document. Decoding uses UseNumber
// so integer literals survive verbatim; encoding re-sorts map keys so struct field
// order and jq key order produce the same bytes.
func CanonBytes(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var tree any
	if err := dec.Decode(&tree); err != nil {
		return nil, fmt.Errorf("wirecheck: decode: %w", err)
	}
	if dec.More() {
		return nil, fmt.Errorf("wirecheck: trailing data after JSON document")
	}
	var buf bytes.Buffer
	if err := writeCanonical(&buf, tree, 0); err != nil {
		return nil, err
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// writeCanonical emits tree in canonical form. Objects are emitted with sorted keys;
// json.Number values are copied byte-for-byte; strings go through json.Marshal so
// escaping stays legal while staying minimal (HTML escaping off by using Marshal on
// the string alone — it never escapes for HTML at this size... it does, so see
// writeString).
func writeCanonical(buf *bytes.Buffer, tree any, depth int) error {
	switch v := tree.(type) {
	case Raw:
		buf.WriteString(string(v))
	case map[string]any:
		buf.WriteString("{\n")
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if err := writeIndent(buf, depth+1); err != nil {
				return err
			}
			if err := writeString(buf, k); err != nil {
				return err
			}
			buf.WriteString(": ")
			if err := writeCanonical(buf, v[k], depth+1); err != nil {
				return err
			}
			if i < len(keys)-1 {
				buf.WriteByte(',')
			}
			buf.WriteByte('\n')
		}
		if err := writeIndent(buf, depth); err != nil {
			return err
		}
		buf.WriteByte('}')
	case []any:
		if len(v) == 0 {
			buf.WriteString("[]")
			return nil
		}
		buf.WriteString("[\n")
		for i, e := range v {
			if err := writeIndent(buf, depth+1); err != nil {
				return err
			}
			if err := writeCanonical(buf, e, depth+1); err != nil {
				return err
			}
			if i < len(v)-1 {
				buf.WriteByte(',')
			}
			buf.WriteByte('\n')
		}
		if err := writeIndent(buf, depth); err != nil {
			return err
		}
		buf.WriteByte(']')
	case json.Number:
		buf.WriteString(v.String())
	case string:
		return writeString(buf, v)
	case bool:
		if v {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case nil:
		buf.WriteString("null")
	default:
		// Non-decoded leaves only reach here from Canonical(v), whose json.Marshal
		// already reduced everything to decoded shapes; reaching this branch means a
		// caller handed raw bytes to a value that Marshal produced oddly. Be loud.
		return fmt.Errorf("wirecheck: unencodable value of type %T", tree)
	}
	return nil
}

func writeIndent(buf *bytes.Buffer, depth int) error {
	for range depth {
		buf.WriteString("  ")
	}
	return nil
}

// writeString escapes s with HTML escaping disabled: a "<" in a published body must
// survive into the golden as "<", not \u003c.
func writeString(buf *bytes.Buffer, s string) error {
	var tmp bytes.Buffer
	enc := json.NewEncoder(&tmp)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return err
	}
	buf.Write(bytes.TrimRight(tmp.Bytes(), "\n"))
	return nil
}
