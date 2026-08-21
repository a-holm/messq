// SPDX-License-Identifier: Apache-2.0

package docsguard

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// linkRE matches an inline markdown link target. Reference-style links do not appear in this
// repository's documents; one would simply not be checked rather than be reported as broken.
var linkRE = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)

// MarkdownFiles lists every markdown file under root, skipping .git and every testdata
// directory. Fixtures are inputs, not documents, and several of them are broken on purpose.
func MarkdownFiles(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "testdata" || d.Name() == "dist" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(d.Name(), ".md") {
			out = append(out, path)
		}
		return nil
	})
	return out, err
}

// BrokenLinksIn returns the relative link targets in one markdown file that do not resolve.
// Absolute URLs and bare fragments are not this check's business.
func BrokenLinksIn(path string) ([]string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(path)

	var broken []string
	for _, m := range linkRE.FindAllStringSubmatch(string(body), -1) {
		target := strings.TrimSpace(m[1])
		if target == "" || strings.HasPrefix(target, "#") || strings.Contains(target, "://") ||
			strings.HasPrefix(target, "mailto:") {
			continue
		}
		if i := strings.IndexByte(target, '#'); i >= 0 {
			target = target[:i]
		}
		if target == "" {
			continue
		}
		//nolint:gosec // G703: the target comes from a document this repository committed, and
		// resolving it is a stat. Nothing is opened, read or written through this path.
		if _, err := os.Stat(filepath.Join(dir, target)); err != nil {
			broken = append(broken, target)
		}
	}
	return broken, nil
}
