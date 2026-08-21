# 0006. Module path

- Status: Accepted
- Date: 2026-08-21
- Relates to: PLAN.md section 3.3, issue #1, issue #4

## Context

The module path appears in every import, in the public import path of `pkg/client`, and in the `-ldflags -X` symbol names, so it is the most expensive string in the repository to change.

## Decision

The module path is `github.com/a-holm/messq`.

The vanity path `go.messq.dev/messq` needs one static page serving a `go-import` meta tag. The domain does not resolve, so the fallback named in issue #1 applies.

## Consequences

Moving the repository to another host later breaks `go get` for external importers of `pkg/client`. Revisiting this is only cheap while `pkg/client` has no external importers.

The full record, including the alternatives table and the migration path if the vanity domain is acquired, is written with the other ADRs in issue #4.
