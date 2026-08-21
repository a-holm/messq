// SPDX-License-Identifier: Apache-2.0

// Package docsguard parses docs/SEMANTICS.md and docs/adr/ and fails the build on drift.
//
// docs/SEMANTICS.md is normative: the conformance suite mirrors its transition table, the
// invariant checker prints its invariant IDs, and later documents cite its section numbers. A
// normative document that nothing checks rots into decoration, so the tables that carry IDs are
// parsed here and the parsed form is asserted against the code it describes.
//
// Parsing and checking are separate on purpose. Every Parse function turns markdown into a
// slice; every Check function is a pure predicate over that slice. The real documents run
// through Parse then Check; the sabotage fixtures under testdata run through the same Check with
// one row broken, which is what proves a checker still bites.
//
// [ParseTransitions] is exported for issue #13: the conformance suite iterates the document and
// asserts that every transition ID appears in at least one test name, so the table stays
// load-bearing in both directions.
package docsguard
