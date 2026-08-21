// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"
)

// govulncheck's SARIF levels. The classification is govulncheck's own, which is why the wrapper
// reads SARIF rather than the JSON stream: reproducing "is this symbol reachable" outside the
// tool would be a second implementation of the only hard part.
const (
	levelCalled   = "error"   // a vulnerable symbol is reachable from messq's own code
	levelImported = "warning" // a vulnerable package is imported, no vulnerable symbol is called
	levelRequired = "note"    // a vulnerable module is required, no vulnerable package is imported
)

// sarifReport is the subset of the SARIF 2.1.0 document govulncheck emits that the gate needs.
type sarifReport struct {
	Runs []struct {
		Results []struct {
			RuleID  string `json:"ruleId"`
			Level   string `json:"level"`
			Message struct {
				Text string `json:"text"`
			} `json:"message"`
		} `json:"results"`
	} `json:"runs"`
}

// finding is one govulncheck result, flattened.
type finding struct {
	OSV     string
	Level   string
	Message string
}

// parseSARIF reads a govulncheck SARIF document. An empty input is an error rather than a pass:
// with -format sarif govulncheck exits 0 whatever it finds, so a crashed scan would otherwise
// read as a clean one.
func parseSARIF(r io.Reader) ([]finding, error) {
	content, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return nil, fmt.Errorf("no SARIF on stdin: govulncheck produced nothing")
	}

	var report sarifReport
	if err := json.Unmarshal(content, &report); err != nil {
		return nil, fmt.Errorf("stdin is not a SARIF document: %w", err)
	}

	var out []finding
	for _, run := range report.Runs {
		for _, res := range run.Results {
			out = append(out, finding{OSV: res.RuleID, Level: res.Level, Message: res.Message.Text})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OSV < out[j].OSV })
	return out, nil
}

// verdict is what the gate decided about one scan.
type verdict struct {
	Blocking   []finding
	Suppressed []finding
	Imported   int
	Required   int
	Unused     []suppression
}

// judge applies the allow file to a scan. Only findings govulncheck calls reachable block the
// build: a vulnerable module that no messq code path reaches is a fact about the dependency
// graph, not about this build, and failing on it would keep the gate permanently red on stdlib
// advisories nobody can act on.
func judge(findings []finding, allow []suppression, now time.Time) verdict {
	live := make(map[string]suppression, len(allow))
	for _, s := range allow {
		if !s.Expired(now) {
			live[s.OSV] = s
		}
	}

	var v verdict
	matched := map[string]bool{}
	for _, f := range findings {
		switch f.Level {
		case levelImported:
			v.Imported++
			continue
		case levelRequired:
			v.Required++
			continue
		case levelCalled:
		default:
			continue
		}

		if _, ok := live[f.OSV]; ok {
			matched[f.OSV] = true
			v.Suppressed = append(v.Suppressed, f)
			continue
		}
		v.Blocking = append(v.Blocking, f)
	}

	for _, s := range allow {
		if !s.Expired(now) && !matched[s.OSV] {
			v.Unused = append(v.Unused, s)
		}
	}
	return v
}
