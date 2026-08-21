# Fixture: prose names an event that is not in the closed vocabulary.

The sweeper writes `msg.acked` when it reaches a delivery whose deadline has passed. The tables
of `deliveries.visible_at` and `consumers.generation` are not event names and must not be
mistaken for them.
