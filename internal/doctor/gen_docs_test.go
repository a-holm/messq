// SPDX-License-Identifier: Apache-2.0

package doctor

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// TestWriteDocsDoctorDoctorFile (re)generates docs/doctor.md when GO_GEN_DOCS=1.
// It doubles as the truth source for TestDocsDoctorHasSectionPerID; keep the
// generated file committed so tools without Go can read it.
func TestWriteDocsDoctorDoctorFile(t *testing.T) {
	if os.Getenv("GO_GEN_DOCS") != "1" {
		t.Skip("set GO_GEN_DOCS=1 to regenerate docs/doctor.md")
	}
	var b strings.Builder
	fmt.Fprint(&b, `<!-- SPDX-License-Identifier: Apache-2.0 -->

# messq doctor — check reference

Every registered check documents itself here. Sections are generated from the
registry's own Summary and Explain strings and enforced by
`+"`TestDocsDoctorHasSectionPerID`"+`: a check that renames or forgets its
teaching paragraph fails the build instead of an incident review.

Findings carry this anchor in their `+"`docs`"+` field (`+
		"`docs/doctor.md#<id>`"+`), so an alert or cron log is always one click from
the paragraph that explains what fired and why it matters.

`)
	for _, id := range DefaultRegistry().List() {
		check := *mustID(t, id)
		fmt.Fprintf(&b, "## %s\n\n**Summary:** %s\n\n%s\n", id, check.Summary, check.Explain)
		b.WriteString("\n")
	}
	if wErr := os.WriteFile("../../docs/doctor.md", []byte(b.String()), 0o600); wErr != nil {
		t.Fatalf("write docs/doctor.md: %v", wErr)
	}
}
