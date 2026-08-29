// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func f(id string, sev Severity, title string) Finding {
	return Finding{ID: id, Severity: sev, Title: title}
}

func TestSortFindingsSeverityDescThenIDThenSubject(t *testing.T) {
	in := []Finding{
		f("server.restored", SevInfo, "restored"),
		f("dlq.zeta", SevFail, "deep"),
		f("stream.a", SevInfo, "alpha-stream"),
		f("consumer.b", SevWarn, "beta"),
		f("consumer.a", SevWarn, "same-id"),
		f("ok.check", SevOK, "fine"),
		f("skipped.x", SevSkipped, "later"),
	}
	want := []string{
		"dlq.zeta",        // fail first
		"consumer.a",      // warns tie-break by subject stream asc
		"consumer.b",      //
		"server.restored", // infos tie-break by id asc
		"stream.a",        //
		"ok.check",        // oks near last
		"skipped.x",       // skips always dead last — noise must not hide action
	}

	got := SortFindings(in)
	gotIDs := make([]string, len(got))
	for i, g := range got {
		gotIDs[i] = g.ID
	}
	if !reflect.DeepEqual(gotIDs, want) {
		t.Fatalf("SortFindings order =\n%v\nwant\n%v", gotIDs, want)
	}
	if !reflect.DeepEqual(in[0], Finding{ID: "server.restored", Severity: SevInfo, Title: "restored"}) {
		t.Fatal("SortFindings must not mutate its input slice")
	}
}

func TestSortFindingsTieBreaksOnConsumerWithinSameStream(t *testing.T) {
	mk := func(stream, consumer string) Finding {
		return Finding{
			ID: "consumer.idle", Severity: SevWarn,
			Subject: Subject{Stream: stream, Consumer: consumer},
		}
	}
	in := []Finding{mk("orders", "zeta"), mk("orders", "alpha"), mk("audit", "mid")}
	out := SortFindings(in)
	want := []string{"audit|mid", "orders|alpha", "orders|zeta"}
	for i, wf := range want {
		parts := strings.SplitN(wf, "|", 2)
		if out[i].Subject.Stream != parts[0] || out[i].Subject.Consumer != parts[1] {
			t.Fatalf("position %d = %+v, want %s/%s", i, out[i].Subject, parts[0], parts[1])
		}
	}
}

func TestHoistReadonlyLatchFirst(t *testing.T) {
	in := []Finding{
		f("server.restart_loop", SevFail, "looping"),
		f("storage.readonly_latch", SevFail, "latched"),
		f("consumer.max_deliver_unlimited", SevWarn, "warn"),
		f("durability.pragma", SevFail, "pragma"),
	}
	out := HoistReadonlyLatch(SortFindings(in))
	if out[0].ID != "storage.readonly_latch" {
		t.Fatalf("first finding = %q, want the read-only latch hoisted above everything", out[0].ID)
	}
	for _, g := range out[1:] {
		if g.ID == "storage.readonly_latch" {
			t.Fatal("latch finding appears twice after hoisting")
		}
	}
	if out[len(out)-1].ID != "consumer.max_deliver_unlimited" {
		t.Fatalf("remaining findings lost their relative order: %+v", out)
	}
}

func TestHoistReadonlyLatchNoopWithoutLatch(t *testing.T) {
	in := SortFindings([]Finding{
		f("b.warn", SevWarn, "w"), f("a.fail", SevFail, "f"),
	})
	got := HoistReadonlyLatch(in)
	if len(got) != 2 || got[0].ID != "a.fail" || got[1].ID != "b.warn" {
		t.Fatalf("hoist without a latch changed the order: %+v", got)
	}
}

func TestSummarizeCountsAndExitCode(t *testing.T) {
	findings := []Finding{
		f("a", SevFail, ""), f("b", SevWarn, ""), f("c", SevWarn, ""),
		f("d", SevInfo, ""), f("e", SevOK, ""), f("g", SevSkipped, ""),
	}
	sum := Summarize(findings, 44, 1904*time.Millisecond)
	if sum.Checks != 44 || sum.OK != 1 || sum.Info != 1 || sum.Warn != 2 ||
		sum.Fail != 1 || sum.Skipped != 1 {
		t.Fatalf("Summarize = %+v", sum)
	}
	if sum.DurationMS != 1904 {
		t.Fatalf("DurationMS = %d, want 1904", sum.DurationMS)
	}
	cases := map[string]int{"warn": 1, "fail": 1, "never": 0}
	for failOn, want := range cases {
		if got := sum.ExitCodeFor(failOn); got != want {
			t.Fatalf("ExitCode(%q) = %d, want %d", failOn, got, want)
		}
	}
	// Nothing above info: exit 0 even under --fail-on warn.
	calm := Summarize([]Finding{f("info.only", SevInfo, "")}, 3, time.Second)
	if calm.ExitCodeFor("warn") != 0 {
		t.Fatalf("info-only ExitCode(warn) = %d, want 0", calm.ExitCodeFor("warn"))
	}
}

func TestParseFailOn(t *testing.T) {
	warnSev, err := ParseFailOn("warn")
	if err != nil || warnSev != SevWarn {
		t.Fatalf("ParseFailOn(warn) = %v,%v", warnSev, err)
	}
	failSev, err := ParseFailOn("fail")
	if err != nil || failSev != SevFail {
		t.Fatalf("ParseFailOn(fail) = %v,%v", failSev, err)
	}
	if _, err := ParseFailOn("sometimes"); err == nil {
		t.Fatal("ParseFailOn accepted an undocumented threshold")
	}
}

func TestJSONDocumentShape(t *testing.T) {
	rep := Report{
		GeneratedAt: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC),
		Source:      SourceLive,
		Target:      Target{Addr: "unix:///run/messq/messq.sock", DataDir: "/var/lib/messq", Version: "1.0.0"},
		Findings: []Finding{{
			ID: "consumer.ack_wait_below_p99", Severity: SevWarn,
			Subject: Subject{Stream: "orders", Consumer: "invoices"},
			Title:   "ack_wait is below the observed ack p99",
			Fix:     []string{"messq consumer edit orders invoices --ack-wait 30s"},
			Evidence: map[string]any{
				"ack_wait_ms": 5000, "p99_ms": 8900, "samples": 412,
			},
			Docs: "docs/doctor.md#consumer.ack_wait_below_p99",
		}},
		Checks:   40,
		Duration: 1904 * time.Millisecond,
	}

	doc, mErr := json.Marshal(JSONDocument(rep))
	if mErr != nil {
		t.Fatalf("marshal json document: %v", mErr)
	}
	var top map[string]any
	if uErr := json.Unmarshal(doc, &top); uErr != nil {
		t.Fatalf("unmarshal: %v", uErr)
	}
	for _, key := range []string{"schema", "generated_at", "source", "target", "findings", "summary"} {
		if _, ok := top[key]; !ok {
			t.Fatalf("json document lacks frozen key %q: %s", key, doc)
		}
	}
	if top["schema"] != float64(1) {
		t.Fatalf("schema = %v, want 1", top["schema"])
	}
	if top["generated_at"] != float64(rep.GeneratedAt.UnixMilli()) {
		t.Fatalf("generated_at = %v, want epoch millis %d", top["generated_at"], rep.GeneratedAt.UnixMilli())
	}
	if top["source"] != "live" {
		t.Fatalf("source = %v, want \"live\"", top["source"])
	}
	target, targetOK := top["target"].(map[string]any)
	if !targetOK {
		t.Fatalf("target missing or not an object: %v", top["target"])
	}
	if target["addr"] != "unix:///run/messq/messq.sock" || target["data_dir"] != "/var/lib/messq" {
		t.Fatalf("target = %v", target)
	}

	items, itemsOK := top["findings"].([]any)
	if !itemsOK || len(items) == 0 {
		t.Fatalf("findings missing or empty: %v", top["findings"])
	}
	finding, findingOK := items[0].(map[string]any)
	if !findingOK {
		t.Fatalf("first finding is not an object: %v", items[0])
	}
	for _, key := range []string{"id", "severity", "subject", "title", "fix", "evidence", "docs"} {
		if _, ok := finding[key]; !ok {
			t.Fatalf("finding lacks frozen key %q: %v", key, finding)
		}
	}
	if finding["severity"] != "warn" {
		t.Fatalf("severity renders as %v, want \"warn\"", finding["severity"])
	}
	if finding["docs"] != "docs/doctor.md#consumer.ack_wait_below_p99" {
		t.Fatalf("docs = %v", finding["docs"])
	}

	sum, sumOK := top["summary"].(map[string]any)
	if !sumOK {
		t.Fatalf("summary missing or not an object: %v", top["summary"])
	}
	for _, key := range []string{"checks", "ok", "info", "warn", "fail", "skipped", "duration_ms", "exit_code"} {
		if _, ok := sum[key]; !ok {
			t.Fatalf("summary lacks frozen key %q: %v", key, sum)
		}
	}
	if sum["exit_code"] != float64(1) {
		t.Fatalf("summary.exit_code = %v, want 1 (a warn exists under default --fail-on warn)", sum["exit_code"])
	}
}

func TestJSONDocumentSourceNamesDataDir(t *testing.T) {
	rep := Report{Source: SourceDataDir}
	doc, mErr := json.Marshal(JSONDocument(rep))
	if mErr != nil {
		t.Fatalf("marshal: %v", mErr)
	}
	if !strings.Contains(string(doc), `"source":"data-dir"`) {
		t.Fatalf("offline document names its source wrong: %s", doc)
	}
}

func TestJSONDocumentEmptySourceOmitsTarget(t *testing.T) {
	rep := Report{}
	doc, mErr := json.Marshal(JSONDocument(rep))
	if mErr != nil {
		t.Fatalf("marshal: %v", mErr)
	}
	if strings.Contains(string(doc), "\"target\":{") {
		t.Fatalf("empty target still occupies the document: %s", doc)
	}
}

func TestNDJSONRecordsMatchSortedFindings(t *testing.T) {
	rep := Report{
		Findings: []Finding{
			f("zeta.warn", SevWarn, "one"), f("alpha.fail", SevFail, "two"),
		},
	}
	recs := NDJSONRecords(rep)
	if len(recs) != 2 {
		t.Fatalf("%d records, want one per finding", len(recs))
	}
	first, mErr := json.Marshal(recs[0])
	if mErr != nil {
		t.Fatalf("marshal record: %v", mErr)
	}
	if !strings.Contains(string(first), `"id":"alpha.fail"`) || !strings.Contains(string(first), `"severity":"fail"`) {
		t.Fatalf("first ndjson record should be the fail finding: %s", first)
	}
}

func TestWriteHumanFaceDeterministic(t *testing.T) {
	rep := Report{
		GeneratedAt: time.Date(2026, 11, 4, 9, 30, 0, 0, time.UTC),
		Source:      SourceLive,
		Target:      Target{Addr: "unix:///run/messq/messq.sock", Version: "1.0.0"},
		Duration:    1904 * time.Millisecond,
		Findings: []Finding{
			{
				ID: "consumer.ack_wait_below_p99", Severity: SevWarn,
				Subject: Subject{Stream: "orders", Consumer: "invoices"},
				Title:   "ack_wait 5s is below the observed ack p99 of 8.9s",
				Detail:  "31% of deliveries time out and are redelivered.",
				Fix:     []string{"messq consumer edit orders invoices --ack-wait 30s"},
				Evidence: map[string]any{
					"ack_wait_ms": 5000, "p99_ms": 8900, "samples": 412,
				},
				Docs: "docs/doctor.md#consumer.ack_wait_below_p99",
			},
			{
				ID: "dlq.growing_undrained", Severity: SevFail,
				Subject: Subject{Stream: "orders"},
				Title:   "DLQ grew 12 to 41 in the window and nothing has redriven it",
				Fix:     []string{"messq dlq ls orders --group-by cause"},
				Docs:    "docs/doctor.md#dlq.growing_undrained",
			},
			{
				ID:       "durability.fsync_probe",
				Severity: SevOK,
				Title:    "fdatasync p50 78µs p99 240µs (1000 samples)",
				NoFix:    "informational — nobody has to trust our README",
				Docs:     "docs/doctor.md#durability.fsync_probe",
			},
			{
				ID: "storage.wal_size", Severity: SevSkipped,
				Title: "needs metrics doctor cannot see offline",
				NoFix: "this check needs a running daemon (try --addr)",
				Docs:  "docs/doctor.md#storage.wal_size",
			},
		},
		Checks: 40,
	}

	var buf bytes.Buffer
	if wErr := WriteHuman(&buf, rep, HumanOpts{}); wErr != nil {
		t.Fatalf("WriteHuman: %v", wErr)
	}
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")

	if lines[0] != "messq doctor   daemon 1.0.0 at unix:///run/messq/messq.sock" {
		t.Fatalf("header line = %q", lines[0])
	}
	idx := func(prefix string) int {
		for i, l := range lines {
			if strings.HasPrefix(l, prefix) {
				return i
			}
		}
		return -1
	}
	dlqAt := idx("[fail]")
	warnAt := idx("[warn]")
	okAt := idx("[ok]")
	skipAt := idx("[skip]")
	if dlqAt < 0 || warnAt < 0 || okAt < 0 || skipAt < 0 {
		t.Fatalf("expected [fail]/[warn]/[ok]/[skip] blocks in:\n%s", buf.String())
	}
	if dlqAt >= warnAt || warnAt >= okAt || okAt >= skipAt {
		t.Fatalf("face ordering broke: fail@%d warn@%d ok@%d skip@%d\n%s",
			dlqAt, warnAt, okAt, skipAt, buf.String())
	}
	// Fix commands carry their own arrow line, indented like prose but greppable.
	if idx("       -> messq dlq ls orders --group-by cause") < 0 {
		t.Fatalf("fix command missing from the human face:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "2 findings need attention (1 fail, 1 warn, 0 info)") {
		t.Fatalf("summary line missing or wrong:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), " · 40 checks · 1.9s") {
		t.Fatalf("checks/duration trailer missing:\n%s", buf.String())
	}
}

func TestWriteHumanQuietDropsOKAndInfo(t *testing.T) {
	rep := Report{
		Duration: time.Second,
		Findings: []Finding{
			f("loud.fail", SevFail, "broken"),
			f("chatty.info", SevInfo, "fyi"),
			f("green.ok", SevOK, "fine"),
			f("maybe.skip", SevSkipped, "later"),
		},
		Checks: 4,
	}
	var buf bytes.Buffer
	if wErr := WriteHuman(&buf, rep, HumanOpts{Quiet: true}); wErr != nil {
		t.Fatalf("WriteHuman quiet: %v", wErr)
	}
	out := buf.String()
	for _, banned := range []string{"chatty.info", "green.ok"} {
		if strings.Contains(out, banned) {
			t.Fatalf("quiet face still shows %q:\n%s", banned, out)
		}
	}
	for _, kept := range []string{"[fail] broken", "maybe.skip — later"} {
		if !strings.Contains(out, kept) {
			t.Fatalf("quiet face dropped %q which quiet keeps:\n%s", kept, out)
		}
	}
}

func TestWriteHumanSingularAndPluralCounts(t *testing.T) {
	mkRep := func(fs []Finding) Report {
		return Report{Duration: time.Second, Findings: fs, Checks: 4}
	}
	var buf bytes.Buffer
	wErr := WriteHuman(&buf, mkRep([]Finding{f("a", SevFail, ""), f("b", SevWarn, "")}), HumanOpts{})
	if wErr != nil {
		t.Fatalf("write: %v", wErr)
	}
	if !strings.Contains(buf.String(), "2 findings need attention (1 fail, 1 warn") {
		t.Fatalf("plural summary wrong:\n%s", buf.String())
	}

	buf.Reset()
	allClear := mkRep([]Finding{f("calm", SevOK, "all good")})
	if hErr := WriteHuman(&buf, allClear, HumanOpts{}); hErr != nil {
		t.Fatalf("write clean: %v", hErr)
	}
	if !strings.Contains(buf.String(), "no findings need attention") {
		t.Fatalf("clean summary wrong:\n%s", buf.String())
	}
}
