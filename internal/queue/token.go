// SPDX-License-Identifier: Apache-2.0

package queue

import "fmt"

// Token is the fenced, human-readable ack token of D7 (issue #9 §7). String renders
// the grammar "#4 §S3": "stream/consumer/seq/attempt/generation", where attempt is
// the POST-increment attempt count (during attempt n the token carries n) and
// generation is the consumer's current generation. Parsing and fencing land in #10;
// this issue only mints tokens, so it commits the corpus #10's fuzz target seeds from
// (testdata/tokens/valid.txt).
type Token struct {
	Stream     string
	Consumer   string
	Seq        int64
	Attempt    int32
	Generation int32
}

// String renders the token in its wire form. The stream and consumer names are
// guaranteed slash-free by rule S11 (and the consumer-name grammar's '/' ban), so the
// four slashes are unambiguous field separators.
func (t Token) String() string {
	return fmt.Sprintf("%s/%s/%d/%d/%d", t.Stream, t.Consumer, t.Seq, t.Attempt, t.Generation)
}
