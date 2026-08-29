// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"

	"github.com/a-holm/messq/internal/errs"
	"github.com/a-holm/messq/internal/queue"
	"github.com/a-holm/messq/internal/store"
)

// actorDevAutocreate is the audit actor of everything serve --dev created on the
// caller's behalf (issue #26 §2): messq events shows exactly how the state
// appeared, and no event vocabulary is invented — the normal stream.create /
// consumer.create events carry this actor.
const actorDevAutocreate = "dev-autocreate"

// devAutocreateStream creates the named stream with schema defaults when it does
// not exist. The wildcard subject set is deliberate: guessing a pattern from one
// subject would be a worse surprise than accepting everything in a mode whose
// data is thrown away (issue §2). Concurrent publishes to the same new stream
// collapse to one create: the store's conflict path reports existed without a
// second event.
//
// It returns the (possibly freshly created) stream info.
func (s *Server) devAutocreateStream(ctx context.Context, name string) (store.StreamInfo, error) {
	cfg := queue.DefaultConfig(name)
	_, existed, err := s.store.CreateStream(ctx, cfg, actorDevAutocreate)
	if err != nil {
		var existsErr *store.StreamExistsError
		if !errors.As(err, &existsErr) {
			return store.StreamInfo{}, err
		}
		// A concurrent request won the create; fall through to the read.
		existed = true
	}
	if !existed {
		s.logger.Warn("stream.create",
			"stream", name, "actor", actorDevAutocreate, "subjects", cfg.Subjects,
			"note", "auto-created because --dev is set",
			"hint", "in production, create it explicitly: messq stream add "+name+" --subjects 'orders.>'")
	}
	return s.store.GetStream(ctx, name)
}

// devAutocreateConsumer creates the named consumer with schema defaults when it
// does not exist (issue #26 §2), logging the same WARN shape as streams. It
// starts at "first": a dev consumer that cannot see the messages it just
// published would teach the wrong lesson.
func (s *Server) devAutocreateConsumer(ctx context.Context, stream, name string) error {
	req := defaultConsumerConfigRequest()
	req.Name = name
	start := queue.StartPosition{Kind: queue.StartFirst}
	if _, err := s.store.CreateConsumer(ctx, stream, req.config(stream), start, actorDevAutocreate); err != nil {
		return err
	}
	s.logger.Warn("consumer.create",
		"stream", stream, "consumer", name, "actor", actorDevAutocreate,
		"note", "auto-created because --dev is set",
		"hint", "in production, create it explicitly: messq consumer add "+stream+" <name>")
	return nil
}

// isNotFound reports whether err is the store's not-found answer.
func isNotFound(err error) bool { return errors.Is(err, errs.ErrNotFound) }
