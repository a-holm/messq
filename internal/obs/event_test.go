// SPDX-License-Identifier: Apache-2.0

package obs

import "testing"

// TestNopSinkAcceptsEvents pins the default sink contract: publishing into NopSink never
// panics and never blocks, whatever it is handed — including an empty slice, which the
// writer must be allowed to skip sending at all.
func TestNopSinkAcceptsEvents(t *testing.T) {
	var sink Sink = NopSink{}
	sink.Publish(nil)
	sink.Publish([]Event{{Event: "msg.publish"}})
	sink.Publish(make([]Event, 0))
}

// TestEventCarriesTheProjectionFields checks the minimal projection struct this issue
// needs: the vocabulary name, the batch timestamp, and the identity fields later issues
// fill in. The full field schema is #19's; renaming any of these would break it.
func TestEventCarriesTheProjectionFields(t *testing.T) {
	e := Event{
		Event: "msg.publish",
		TS:    1_700_000_000_000,
		MsgID: "01JTEST",
	}
	if e.Event != "msg.publish" || e.TS != 1_700_000_000_000 || e.MsgID != "01JTEST" {
		t.Fatalf("event fields did not round-trip: %+v", e)
	}
}
