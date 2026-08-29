// SPDX-License-Identifier: Apache-2.0

package help

import (
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/messq/internal/cli/exit"
	"github.com/a-holm/messq/internal/lifecycle"
)

// TestEveryTopicRenders is the floor: every registered topic renders without
// error and carries its name as a heading.
func TestEveryTopicRenders(t *testing.T) {
	for _, topic := range All() {
		r := Render(topic.Source, false)
		if r == "" {
			t.Errorf("topic %s rendered empty", topic.Name)
		}
		if strings.Contains(r, "\x1b") {
			t.Errorf("topic %s rendered ANSI without colour enabled", topic.Name)
		}
		if !IsTopic(topic.Name) {
			t.Errorf("topic %s not reachable via IsTopic", topic.Name)
		}
	}
}

// TestExitCodeTopicMatchesCode is THE generated-topic binding: the exit-codes
// topic must name every documented code with its exact name and meaning, and
// nothing else. exit.Documented panics on a half-row (a code with no meaning),
// so "adding a code without a description fails the build" is literal.
func TestExitCodeTopicMatchesCode(t *testing.T) {
	source := exitCodesSource()
	for _, d := range exitDocumented() {
		wantRow := fmt.Sprintf("| %d | `%s` | %s |", d.Code, d.Name, d.Meaning)
		if !strings.Contains(source, wantRow) {
			t.Errorf("exit-codes topic is missing the exact row %q", wantRow)
		}
	}
	// No invented rows: every | N | table row in the topic must come from the
	// documented set (74/75/78/130 appear in prose, not as table rows).
	documented := map[int]bool{}
	for _, d := range exitDocumented() {
		documented[d.Code] = true
	}
	for _, line := range strings.Split(source, "\n") {
		if !strings.HasPrefix(line, "| ") {
			continue
		}
		fields := strings.Split(strings.TrimPrefix(line, "| "), " |")
		var code int
		if _, err := fmt.Sscanf(fields[0], "%d", &code); err != nil {
			continue
		}
		if !documented[code] {
			t.Errorf("exit-codes topic documents code %d, which the exit package does not", code)
		}
	}
	// The exceptions sit outside the table, in prose, verbatim.
	for _, exception := range []string{"74", "75", "78", "130"} {
		if !strings.Contains(source, exception) {
			t.Errorf("exit-codes topic lost the documented exception %s", exception)
		}
	}
}

// TestExitCodeTopicStaysInSyncWithGeneratedDoc is the second half of the
// generation contract: the topic's table and docs/generated/exit-codes.md are
// two renders of ONE table. If they disagree, one of them drifted.
func TestExitCodeTopicStaysInSyncWithGeneratedDoc(t *testing.T) {
	for _, d := range exitDocumented() {
		if exit.Name(d.Code) != d.Name {
			t.Errorf("Name(%d) = %q, want %q", d.Code, exit.Name(d.Code), d.Name)
		}
	}
}

// TestLifecycleTopicCoversEveryTransition walks internal/lifecycle's closed
// transition table and requires every legal move to appear in the lifecycle
// topic. An unmentioned transition (or a new state) fails here — the topic
// cannot drift from the state machine.
func TestLifecycleTopicCoversEveryTransition(t *testing.T) {
	source, err := Get("lifecycle")
	if err != nil {
		t.Fatalf("lifecycle topic: %v", err)
	}
	text := source.Source
	for _, tr := range lifecycle.Transitions() {
		// The topic spells transitions "FROM | TO" inside its table.
		pair := fmt.Sprintf("| %s | %s |", tr.From.String(), tr.To.String())
		if !strings.Contains(text, pair) {
			t.Errorf("lifecycle topic does not mention the legal transition %s → %s",
				tr.From, tr.To)
		}
	}
	// And no invented transitions: every from|to table row must be legal.
	legal := map[string]bool{}
	for _, tr := range lifecycle.Transitions() {
		legal[fmt.Sprintf("| %s | %s |", tr.From.String(), tr.To.String())] = true
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "| ") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) < 2 {
			continue
		}
		from := strings.TrimSpace(cells[0])
		to := strings.TrimSpace(cells[1])
		candidate := fmt.Sprintf("| %s | %s |", from, to)
		if isLifecycleStateRow(from, to) && !legal[candidate] {
			t.Errorf("lifecycle topic documents transition %s → %s, which the state machine forbids", from, to)
		}
	}
}

// isLifecycleStateRow filters the topic's table rows down to the from|to|cause
// table (the message-lifecycle prose table has different columns).
func isLifecycleStateRow(from, to string) bool {
	states := map[string]bool{
		"STARTING": true, "RECOVERING": true, "READY": true,
		"DRAINING": true, "STOPPED": true, "FATAL": true,
	}
	return states[from] && states[to]
}

// TestUnknownTopicErrorListsTopics pins the teaching error's shape: the message
// names the bad topic AND lists the real ones (the CLI turns this into exit 2).
func TestUnknownTopicErrorListsTopics(t *testing.T) {
	_, err := Get("nosuchtopic")
	if err == nil {
		t.Fatal("Get(nosuchtopic) unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "nosuchtopic") || !strings.Contains(err.Error(), "concepts") {
		t.Errorf("unknown-topic error must name the topic and the list: %v", err)
	}
}
