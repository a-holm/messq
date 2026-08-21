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
// # What is checked, and what is not
//
// PLAN.md is the other side of most comparisons. Invariant statements are compared verbatim
// against section 5.2, transition IDs and emitted events against section 5.1, the event
// vocabulary against section 9.2, and the set of adjudicated decisions against section 2. The
// error outcome table is compared against internal/errs's source, sentinel by sentinel and
// message by message, in declaration order.
//
// Transition guards and effects are not compared as text, because no tractable grammar separates
// a reworded cell from a changed rule. Their symbols are: a flag, a reserved header, a dotted
// name, a snake_case identifier or a relational operator that PLAN.md names must still appear in
// the specification, or be quoted in the S1.5 register entry that explains its removal. A symbol
// the specification adds is left to review, and S6.1 says so rather than implying a stronger
// guarantee than this package delivers.
//
// [ParseTransitions] is exported for issue #13: the conformance suite iterates the document and
// asserts that every transition ID appears in at least one test name, so the table stays
// load-bearing in both directions.
package docsguard
