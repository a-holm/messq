# Architecture decision records

An ADR records one decision that is expensive to reverse: what was decided, the alternatives, and the consequences the project accepts. Decisions that are cheap to change live in the code.

## Numbering

Files are named `NNNN-kebab-case-title.md` with a four-digit, zero-padded number. Numbers are allocated in the order decisions are taken and are never reused, so a link to `0006` always means the same decision. `0000-template.md` is the template and is not a decision.

Numbers `0002` through `0005` are allocated to the adjudicated decisions of PLAN.md section 2 and are written in issue #4.

## Status

An ADR is `Proposed`, `Accepted`, `Superseded by NNNN` or `Deprecated`. A superseded ADR stays in place with its status changed; a new record explains the replacement. ADRs are not edited to describe a later decision.

## Writing one

Copy `0000-template.md`, take the next free number, and keep the record short. The reasoning matters more than the prose.
