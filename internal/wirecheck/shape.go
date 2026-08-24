// SPDX-License-Identifier: Apache-2.0

package wirecheck

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Presence is whether a field always appears on the wire or may be absent.
// `always` vs `optional` is omitempty — contract, not formatting (issue #18 §3).
type Presence string

const (
	Always   Presence = "always"
	Optional Presence = "optional"
)

// JSON kinds used in digests. "json" marks an opaque value: a custom MarshalJSON type
// declared explicitly, or any-typed interface.
const (
	kindString = "string"
	kindNumber = "number"
	kindBool   = "bool"
	kindJSON   = "json"
	kindObject = "object"
)

// Digest is one wire type's field-level shape, sorted by JSON path so struct field
// reordering — not a wire change — never churns a committed digest.
type Digest struct {
	// Type is the Go type name the digest was generated from.
	Type string
	// Fields holds one row per JSON-visible leaf, sorted by Path.
	Fields []Field
}

// Field is one line of a .shape file.
type Field struct {
	Path     string // dotted JSON path; "[]" marks array elements, "<key>" map keys
	Kind     string // string | number | bool | object | json
	Presence Presence
	Notes    string // extra contract: int64, int32, base64, map[string]string, custom-marshaler
}

// Has reports whether path appears in the digest.
func (d Digest) Has(path string) bool {
	for _, f := range d.Fields {
		if f.Path == path {
			return true
		}
	}
	return false
}

const digestColumn = 20

// Render renders the digest in its committed .shape form: aligned columns,
// whitespace-significant only between the fixed columns, diff-friendly.
func (d Digest) Render() string {
	lines := make([]string, 0, len(d.Fields))
	for _, f := range d.Fields {
		line := fmt.Sprintf("%-*s %-8s %-8s%s", digestColumn, f.Path, f.Kind, f.Presence, notesField(f.Notes))
		lines = append(lines, strings.TrimRight(line, " "))
	}
	return strings.Join(lines, "\n")
}

// notesField renders the optional notes column with its three-space separator.
func notesField(n string) string {
	if n == "" {
		return ""
	}
	return "   " + n
}

// ParseDigest parses Render output back into a Digest. Hand-edited lines that do not
// parse are errors, never silently-ignored content.
func ParseDigest(rendered string) (Digest, error) {
	var d Digest
	for _, line := range strings.Split(rendered, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 3 {
			return Digest{}, fmt.Errorf("wirecheck: malformed digest line %q", line)
		}
		presence := Presence(parts[2])
		switch presence {
		case Always, Optional:
		default:
			return Digest{}, fmt.Errorf("wirecheck: unknown presence %q in line %q", parts[2], line)
		}
		d.Fields = append(d.Fields, Field{
			Path:     parts[0],
			Kind:     parts[1],
			Presence: presence,
			Notes:    strings.Join(parts[3:], " "),
		})
	}
	sort.Slice(d.Fields, func(i, j int) bool { return d.Fields[i].Path < d.Fields[j].Path })
	return d, nil
}

var (
	marshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	textMarshaler = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

// customMarshaler reports whether t encodes itself. Such a type's wire bytes cannot be
// inferred from its fields, so its digest entry declares it explicitly instead of
// guessing (issue #18 §1 G1).
func customMarshaler(t reflect.Type) bool {
	return t.Implements(marshalerType) || reflect.PointerTo(t).Implements(marshalerType) ||
		t.Implements(textMarshaler) || reflect.PointerTo(t).Implements(textMarshaler)
}

// jsonKinds maps every reflect.Kind to the JSON type encoding/json renders. The
// non-scalar composite kinds (array/object) and the opaque kinds (json) are all
// spelled out so the enum map stays complete under the exhaustive linter.
var jsonKinds = map[reflect.Kind]string{
	reflect.Invalid:       kindJSON,
	reflect.Bool:          kindBool,
	reflect.Int:           kindNumber,
	reflect.Int8:          kindNumber,
	reflect.Int16:         kindNumber,
	reflect.Int32:         kindNumber,
	reflect.Int64:         kindNumber,
	reflect.Uint:          kindNumber,
	reflect.Uint8:         kindNumber,
	reflect.Uint16:        kindNumber,
	reflect.Uint32:        kindNumber,
	reflect.Uint64:        kindNumber,
	reflect.Uintptr:       kindNumber,
	reflect.Float32:       kindNumber,
	reflect.Float64:       kindNumber,
	reflect.Complex64:     kindJSON,
	reflect.Complex128:    kindJSON,
	reflect.Array:         "array",
	reflect.Chan:          kindJSON,
	reflect.Func:          kindJSON,
	reflect.Interface:     kindJSON,
	reflect.Map:           kindObject,
	reflect.Pointer:       kindJSON,
	reflect.Slice:         "array",
	reflect.String:        kindString,
	reflect.Struct:        kindObject,
	reflect.UnsafePointer: kindJSON,
}

func jsonKind(t reflect.Type) string {
	if k, ok := jsonKinds[t.Kind()]; ok {
		return k
	}
	return kindJSON
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	if name == "" {
		return prefix
	}
	return prefix + "." + name
}

// DigestOf reflects over one example value of a wire type and returns its digest. The
// argument is only a type carrier; a zero value is fine.
func DigestOf(v any) (Digest, error) {
	t := reflect.TypeOf(v)
	d := Digest{Type: t.Name()}
	if err := walkType(t, "", Always, &d, map[reflect.Type]bool{}); err != nil {
		return Digest{}, err
	}
	sort.Slice(d.Fields, func(i, j int) bool { return d.Fields[i].Path < d.Fields[j].Path })
	return d, nil
}

// walkType appends the JSON-visible leaves of t to d. path is the full JSON path of
// the container itself ("" at the root); named fields extend it with joinPath,
// arrays with "[]", maps with "<key>".
func walkType(t reflect.Type, path string, presence Presence, d *Digest, seen map[reflect.Type]bool) error {
	for t.Kind() == reflect.Pointer {
		presence = Optional // nil is representable: the field can vanish or be null
		t = t.Elem()
	}
	if customMarshaler(t) {
		return appendField(d, path, kindJSON, presence, "custom-marshaler")
	}
	kind := t.Kind()
	switch { //nolint:staticcheck // QF1002: a tagged switch here trips exhaustive, whose default-signifies-exhaustive is false by policy
	case kind == reflect.Struct:
		if seen[t] {
			return nil // recursive type: stop at the first cycle
		}
		seen[t] = true
		defer delete(seen, t)
		for i := range t.NumField() {
			f := t.Field(i)
			// An embedded field of unexported type is itself "unexported" by name,
			// yet encoding/json still walks its exported members — so only skip a
			// field when it is neither exported nor an anonymous embedding.
			if !f.IsExported() && !f.Anonymous {
				continue
			}
			name, tags, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			fpres := presence
			if f.Type.Kind() == reflect.Pointer {
				fpres = Optional
			}
			for _, tag := range strings.Split(tags, ",") {
				if tag == "omitempty" {
					fpres = Optional
				}
			}
			if f.Anonymous && name == "" {
				// Embedded struct: its fields inline into the parent's paths.
				if err := walkType(f.Type, path, fpres, d, seen); err != nil {
					return err
				}
				continue
			}
			wireName := name
			if wireName == "" {
				wireName = f.Name
			}
			if err := walkType(f.Type, joinPath(path, wireName), fpres, d, seen); err != nil {
				return err
			}
		}
	case kind == reflect.Slice:
		elem := t.Elem()
		if elem.Kind() == reflect.Uint8 {
			return appendField(d, path, kindString, presence, "base64")
		}
		return walkType(elem, path+"[]", presence, d, seen)
	case kind == reflect.Array:
		return walkType(t.Elem(), path+"[]", presence, d, seen)
	case kind == reflect.Map:
		val := t.Elem()
		note := "map[" + t.Key().Name() + "]" + val.Name()
		kind := jsonKind(val)
		if val.Kind() == reflect.Struct || customMarshaler(val) {
			kind = kindObject
		}
		return appendField(d, joinPath(path, "<key>"), kind, presence, note)
	case kind == reflect.Interface:
		note := ""
		if t.NumMethod() != 0 {
			note = "custom-marshaler"
		}
		return appendField(d, path, kindJSON, presence, note)
	default:
		return appendField(d, path, jsonKind(t), presence, primitiveNote(t))
	}
	return nil
}

func appendField(d *Digest, path, kind string, presence Presence, notes string) error {
	d.Fields = append(d.Fields, Field{Path: path, Kind: kind, Presence: presence, Notes: notes})
	return nil
}

// primitiveNote records the Go-side type of a scalar: int64 widths matter to consumers
// (seq beyond 2^53), and named string types preview their future enum freeze.
func primitiveNote(t reflect.Type) string {
	kind := t.Kind()
	switch {
	case kind == reflect.Bool:
		return ""
	case kind == reflect.String && t.Name() == "string":
		return ""
	case t.Name() != "":
		return t.Name()
	default:
		return ""
	}
}
