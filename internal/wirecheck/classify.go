// SPDX-License-Identifier: Apache-2.0

package wirecheck

import (
	"fmt"
	"sort"
)

// Side says whether the digested type sits in a request (what old clients SEND) or a
// response (what they RECEIVE). It decides one matrix row: an added always-field is
// additive for a server's responses but breaks every old client's requests.
type Side int

const (
	Response Side = iota
	Request
)

// Verdict is the compatibility label a digest diff carries.
type Verdict int

const (
	Additive Verdict = iota
	Breaking
)

func (v Verdict) String() string {
	if v == Breaking {
		return "BREAKING"
	}
	return "ADDITIVE"
}

// ChangeKind names one detected difference.
type ChangeKind string

const (
	Added       ChangeKind = "added"
	Removed     ChangeKind = "removed"
	Retyped     ChangeKind = "retyped"
	Tightened   ChangeKind = "always → optional" // a field that used to always appear can now vanish
	Loosened    ChangeKind = "optional → always"
	NoteChanged ChangeKind = "notes changed"
	Marshaler   ChangeKind = "custom marshaler changed"
)

// Change is one difference between two digests.
type Change struct {
	Kind   ChangeKind
	Path   string
	Detail string
}

// Report is the classifier's full output.
type Report struct {
	Verdict Verdict
	Changes []Change
}

// Summary renders the changes as one greppable line list.
func (r Report) Summary() string {
	s := ""
	for _, c := range r.Changes {
		s += fmt.Sprintf("  - %s field: %s %s\n", c.Kind, c.Path, c.Detail)
	}
	return s
}

// Classify diffs old → new and labels the change. Any BREAKING row makes the whole
// report BREAKING; a removal plus an addition together are reported as a suspected
// rename but stay BREAKING either way.
func Classify(oldD, newD Digest, side Side) Report {
	rep := Report{}
	oldIdx := index(oldD)
	newIdx := index(newD)

	for _, f := range oldD.Fields {
		nf, ok := newIdx[f.Path]
		switch {
		case !ok:
			rep.Changes = append(rep.Changes, Change{
				Kind: Removed, Path: f.Path,
				Detail: fmt.Sprintf("(%s%s)", f.Kind, notesSuffix(f.Notes)),
			})
			rep.Verdict = Breaking
		case nf.Kind != f.Kind:
			rep.Changes = append(rep.Changes, Change{
				Kind: Retyped, Path: f.Path,
				Detail: fmt.Sprintf("%s → %s", f.Kind, nf.Kind),
			})
			rep.Verdict = Breaking
		case f.Presence == Always && nf.Presence == Optional:
			rep.Changes = append(rep.Changes, Change{Kind: Tightened, Path: f.Path})
			rep.Verdict = Breaking
		case f.Presence == Optional && nf.Presence == Always:
			rep.Changes = append(rep.Changes, Change{Kind: Loosened, Path: f.Path})
		case f.Notes != nf.Notes:
			if f.Notes == "custom-marshaler" || nf.Notes == "custom-marshaler" {
				rep.Changes = append(rep.Changes, Change{Kind: Marshaler, Path: f.Path})
				rep.Verdict = Breaking
				continue
			}
			// Go-side scalar width or map annotation changed under the same JSON kind:
			// record it, review it in the diff, do not stop a release over it.
			rep.Changes = append(rep.Changes, Change{
				Kind: NoteChanged, Path: f.Path,
				Detail: fmt.Sprintf("%s → %s", f.Notes, nf.Notes),
			})
		}
	}
	for _, f := range newD.Fields {
		if _, ok := oldIdx[f.Path]; ok {
			continue
		}
		additive := true
		detail := fmt.Sprintf("(%s%s)", f.Kind, notesSuffix(f.Notes))
		if f.Presence == Always && side == Request {
			additive = false // old clients never send it: their requests stop working
			detail += " — required on requests is a break for old clients"
		}
		rep.Changes = append(rep.Changes, Change{Kind: Added, Path: f.Path, Detail: detail})
		if !additive {
			rep.Verdict = Breaking
		}
	}
	sort.Slice(rep.Changes, func(i, j int) bool { return rep.Changes[i].Path < rep.Changes[j].Path })
	return rep
}

// ClassifyEnum diffs two closed string sets. Members may be added (clients must
// tolerate unknown members, stated in PROTOCOL.md); removals and renames break.
func ClassifyEnum(oldE, newE []string) Verdict {
	oldSet, newSet := set(oldE), set(newE)
	for m := range oldSet {
		if !newSet[m] {
			return Breaking
		}
	}
	return Additive
}

// ClassifyStatusTable diffs a code → HTTP status table. A code whose status moved is a
// break even though no shape changed (§7 freezes code → status).
func ClassifyStatusTable(oldT, newT map[string]int) Verdict {
	for code, status := range oldT {
		ns, ok := newT[code]
		if !ok || ns != status {
			return Breaking
		}
	}
	return Additive
}

func index(d Digest) map[string]Field {
	m := make(map[string]Field, len(d.Fields))
	for _, f := range d.Fields {
		m[f.Path] = f
	}
	return m
}

func set(items []string) map[string]bool {
	m := make(map[string]bool, len(items))
	for _, s := range items {
		m[s] = true
	}
	return m
}

func notesSuffix(n string) string {
	if n == "" {
		return ""
	}
	return " " + n
}
