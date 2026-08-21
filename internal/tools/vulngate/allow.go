// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

// dateLayout is the only date format .govulncheck-allow accepts. ISO order sorts correctly and
// leaves no room for a day/month ambiguity in a file that gates a build.
const dateLayout = "2006-01-02"

// osvID matches a Go vulnerability database identifier. A CVE or GHSA alias is not accepted:
// govulncheck reports the GO- id, and matching on anything else would silently suppress
// nothing.
var osvID = regexp.MustCompile(`^GO-\d{4}-\d+$`)

// suppression is one line of .govulncheck-allow.
type suppression struct {
	OSV     string
	Expires time.Time
	Reason  string
}

// Expired reports whether the suppression is past its last valid day. The expiry date itself
// is still valid, so an entry written for "2026-09-30" covers all of 30 September.
func (s suppression) Expired(now time.Time) bool {
	return now.After(s.Expires.AddDate(0, 0, 1).Add(-time.Nanosecond))
}

// parseAllow reads .govulncheck-allow. Every field is mandatory: a suppression without an
// expiry never rots and a suppression without a reason cannot be reviewed.
func parseAllow(r io.Reader) ([]suppression, error) {
	var out []suppression
	seen := map[string]int{}

	scanner := bufio.NewScanner(r)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}

		fields := strings.Fields(text)
		if len(fields) < 3 {
			return nil, fmt.Errorf("line %d: want '<osv-id> <expires> <reason>', got %q", lineNo, text)
		}
		if !osvID.MatchString(fields[0]) {
			return nil, fmt.Errorf("line %d: %q is not a GO-YYYY-NNNN identifier", lineNo, fields[0])
		}
		expires, err := time.Parse(dateLayout, fields[1])
		if err != nil {
			return nil, fmt.Errorf("line %d: expiry %q is not a %s date", lineNo, fields[1], dateLayout)
		}
		if first, dup := seen[fields[0]]; dup {
			return nil, fmt.Errorf("line %d: %s is already suppressed on line %d", lineNo, fields[0], first)
		}
		seen[fields[0]] = lineNo

		out = append(out, suppression{
			OSV:     fields[0],
			Expires: expires,
			Reason:  strings.Join(fields[2:], " "),
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// expiredSuppressions returns the entries that are past their date. They fail the build whether
// or not the vulnerability is still reported: a suppression must rot loudly, or the allow file
// becomes a list of things nobody has looked at since.
func expiredSuppressions(allow []suppression, now time.Time) []suppression {
	var out []suppression
	for _, s := range allow {
		if s.Expired(now) {
			out = append(out, s)
		}
	}
	return out
}
