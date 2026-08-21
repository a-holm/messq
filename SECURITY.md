# Security policy

## Reporting a vulnerability

Report privately through [GitHub private vulnerability reporting](https://github.com/a-holm/messq/security/advisories/new). That form is the disclosure address; it is private to the maintainers and does not create a public issue.

Do not open a public issue, a pull request, or a discussion for a suspected vulnerability.

Include the messq version (`messq version --output json`), the platform, a reproduction, and the impact you believe it has.

## What to expect

| Stage | Target |
|---|---|
| Acknowledgement | 3 working days |
| Initial assessment | 10 working days |
| Fix or mitigation published | within the 90-day disclosure window |

messq follows a 90-day coordinated disclosure policy. Details become public when a fix ships or 90 days after the report, whichever comes first. A reporter who wants to publish earlier is asked to coordinate the date.

There is no bug bounty.

## Supported versions

| Version | Supported |
|---|---|
| `main` | Yes |

Released versions get their own row from the first tag onwards.

## Scope

The threat model that says which properties messq claims, and against whom, is tracked in issue #16.
