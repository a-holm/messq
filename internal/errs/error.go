// SPDX-License-Identifier: Apache-2.0

package errs

import (
	"errors"
	"fmt"
	"slices"
)

// Error is the teaching-error carrier of PLAN.md section 8: what happened, and what to type
// next. It is always used as a pointer, so errors.As recovers it from any depth of wrapping.
type Error struct {
	// Err is the sentinel this failure classifies as. errors.Is walks through here.
	Err error
	// Op names the operation, in package.Function form: "store.Publish", "api.fetch".
	Op string
	// Msg is one human sentence. It carries no payload data and no secrets.
	Msg string
	// Next holds suggested commands, in the order they should be tried.
	Next []string
}

// Error renders "op: msg", or just the message when there is no op.
func (e *Error) Error() string {
	if e.Op == "" {
		return e.Msg
	}
	return e.Op + ": " + e.Msg
}

// Unwrap exposes the sentinel to errors.Is.
func (e *Error) Unwrap() error { return e.Err }

// E builds an Error. The format arguments produce Msg, so the sentinel stays comparable while
// the message stays specific.
func E(sentinel error, op, format string, args ...any) *Error {
	return &Error{Err: sentinel, Op: op, Msg: fmt.Sprintf(format, args...)}
}

// WithNext returns err with next appended to its suggestions. The result wraps err, so
// errors.Is still finds the sentinel and the rendered message is unchanged. err is never
// modified: the caller may already have handed it to someone else.
func WithNext(err error, next ...string) error {
	if err == nil || len(next) == 0 {
		return err
	}
	return &Error{
		Err:  err,
		Msg:  err.Error(),
		Next: append(NextOf(err), next...),
	}
}

// NextOf returns the suggestions carried by err, or nil when it carries none. The result is a
// copy.
func NextOf(err error) []string {
	var e *Error
	if !errors.As(err, &e) || len(e.Next) == 0 {
		return nil
	}
	return slices.Clone(e.Next)
}
