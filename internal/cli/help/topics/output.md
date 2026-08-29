# output

Every command has exactly three faces, and `--output` picks one: `table`,
`json`, `ndjson`. There is never a fourth mode.

**auto** (the default) reads the terminal: table on a TTY, json on a pipe.
`ndjson` wins on a pipe only for streaming commands (`sub`, `events
--follow`), because a followed stream has no final document. Scripts should
never rely on auto: pin the mode.

**Data and narration are separate streams.** stdout carries data and only
data; stderr carries warnings, progress and teaching hints. The `next:` footer
that closes every inspect command is narration on the table face and a
`next[]` array inside the JSON document — data, never loose prose.

**ULIDs are abbreviated** to their first 11 characters in the table face and
full in machine faces; `--full-ids` forces full everywhere. Never shorten one
yourself — pass the abbreviation back; the CLI resolves it.

**Field stability.** `--output json` field names are compatibility surface
from #24 onward: a renamed field is a breaking change, gated by the wire
classifier, never a silent refresh. Colours exist only in the table face, only
on a TTY, and `NO_COLOR=1` disables them everywhere.
