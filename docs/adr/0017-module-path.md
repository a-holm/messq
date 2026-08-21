# 0017. Module path

- Status: Accepted
- Date: 2026-08-21
- Adjudicates: none
- Relates to: PLAN.md section 3.3, issue #1, issue #4

## Context

The module path appears in every import, in the public import path of `pkg/client`, and in the `-ldflags -X` symbol names, so it is the most expensive string in the repository to change.

`pkg/client` is part of the compatibility promise from the first release (ADR-0015), which means an external importer's build breaks if the path moves.

## Decision

The module path is `github.com/a-holm/messq`.

The vanity path `go.messq.dev/messq` needs one static page serving a `go-import` meta tag. The domain does not resolve, so the fallback named in issue #1 applies.

## Alternatives

| Option | Why it was serious | Why it lost |
|---|---|---|
| `go.messq.dev/messq` | A vanity path decouples the import path from the host, so moving the repository later costs nothing, and it is one static page to serve. | The domain does not resolve, so `go get` would fail for every importer on day one. A vanity path is only worth having when it works. |
| A shorter path such as `messq.dev` | Shorter in every import line. | It needs the same unresolved domain, and the module name would then differ from the repository name, which is one more thing to explain. |

## Consequences

Moving the repository to another host later breaks `go get` for external importers of `pkg/client`. Revisiting this is only cheap while `pkg/client` has no external importers.

Migrating to the vanity path if the domain is acquired is mechanical but not free: the `go.mod` module line changes, every import in the tree changes, the `-ldflags -X` symbol paths in the Makefile change, and the old path keeps working only for as long as the host serves a redirect. The migration is worth doing before the first tagged release and not after.

## Revisit trigger

Acquiring `messq.dev` before the first tagged release. After the tag the cost lands on external importers, and the trigger is void.
