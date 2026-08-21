// SPDX-License-Identifier: Apache-2.0

package docsguard

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
)

// Sentinel is one declaration from internal/errs: the identifier the specification cites and the
// message the sentinel carries.
type Sentinel struct {
	Name    string
	Message string
}

// ParseSentinels reads the sentinel declarations out of internal/errs's source, in declaration
// order. The source rather than the values, because a test that only compares messages cannot
// tell errs.ErrNotFound from errs.ErrConflict, and the specification names both.
func ParseSentinels(path string) ([]Sentinel, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("docsguard: %w", err)
	}

	var out []Sentinel
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
				continue
			}
			name := value.Names[0].Name
			if !strings.HasPrefix(name, "Err") {
				continue
			}
			msg, ok := errorsNewLiteral(value.Values[0])
			if !ok {
				return nil, fmt.Errorf("docsguard: %s: %s is not errors.New with a literal message", path, name)
			}
			out = append(out, Sentinel{Name: name, Message: msg})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("docsguard: %s declares no sentinels", path)
	}
	return out, nil
}

// errorsNewLiteral matches errors.New("...") and returns the message.
func errorsNewLiteral(expr ast.Expr) (string, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return "", false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "New" {
		return "", false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "errors" {
		return "", false
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	msg, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return msg, true
}
