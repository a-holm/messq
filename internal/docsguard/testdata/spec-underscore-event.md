# Fixture: prose names an event whose local part carries an underscore.

The settle path writes `msg.ack_dupe` when the delivery is already resolved. Every merged event
name with an underscore, `msg.ack_dup` and `msg.ack_stale`, is a member of the vocabulary, so a
candidate filter that only accepts letters would let this one through.
