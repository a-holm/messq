// SPDX-License-Identifier: Apache-2.0

package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/a-holm/messq/pkg/client"
)

// This file owns issue #22's godoc-example criterion: Example_publish,
// Example_worker and Example_shellParity compile AND run on every `go test`
// (CI included), so the documented surface can never rot silently. The first
// two answer the real wire shapes with a canned daemon — swap its URL for your
// `messq serve` address and they are production code.

// startCannedDaemon speaks just enough of the §7 wire for one publish and one
// delivered message, then holds with "paused" so an idle worker rests instead
// of spinning.
func startCannedDaemon() *httptest.Server {
	var fetched bool
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/streams/orders/messages":
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"stream":"orders","seq":7,"id":"01JEXAMPLE000000000000000000","trace_id":"t-doc","duplicate":false}`)
		case strings.HasSuffix(r.URL.Path, "/consumers/w"):
			_, _ = io.WriteString(w, `{"stream":"orders","name":"w","ack_wait_ms":30000,"max_deliver":5,"max_ack_pending":100}`)
		case strings.HasSuffix(r.URL.Path, "/fetch"):
			var req struct {
				Batch int `json:"batch"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			_ = r.Body.Close()
			if !fetched {
				fetched = true
				_, _ = io.WriteString(w, `{"messages":[{"stream":"orders","consumer":"w","seq":1,"subject":"orders.west",`+
					`"body_b64":"aGVsbG8gbWVzc3E=","size":11,"attempt":1,"max_deliver":5,`+
					`"ack_token":"orders/w/1/1/1","ack_wait_ms":30000}],"pending":0,"backlog":0}`)
				return
			}
			_, _ = io.WriteString(w, `{"hold_reason":"paused","retry_after_ms":1000,"pending":0,"backlog":0}`)
		case r.URL.Path == "/v1/ack":
			var req struct {
				Tokens []string `json:"tokens"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			_ = r.Body.Close()
			results := make([]string, len(req.Tokens))
			for i, tok := range req.Tokens {
				results[i] = `{"token":"` + tok + `","result":"ok"}`
			}
			_, _ = io.WriteString(w, `{"results":[`+strings.Join(results, ",")+`],"ok":`+fmt.Sprint(len(results))+`}`)
		default:
			http.NotFound(w, r)
		}
	}))
}

// Publishing is one call: subject plus bytes, an optional dedup key, one typed
// receipt back.
func Example_publish() {
	srv := startCannedDaemon()
	defer srv.Close()

	c, err := client.New(srv.URL) // or "tcp://host:port" / "unix:///run/messq.sock"
	if err != nil {
		log.Fatal(err)
	}
	ack, err := c.Publish(context.Background(), "orders", client.Msg{
		Subject: "orders.west",
		Body:    []byte("hello messq"),
		MsgID:   "order-42", // retry-safe: a re-publish dedups instead of duplicating
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("seq", ack.Seq, "duplicate:", ack.Duplicate)
	// Output: seq 7 duplicate: false
}

// The Worker owns the at-least-once footguns so handlers stay plain functions:
// leases are extended while you work, outcomes settle once, draining naks what
// it held. Give it a context that outlives the work, and Drain when shutting down.
func Example_worker() {
	srv := startCannedDaemon()
	defer srv.Close()

	c, err := client.New(srv.URL)
	if err != nil {
		log.Fatal(err)
	}
	w, err := c.NewWorker(client.WorkerConfig{Stream: "orders", Consumer: "w"})
	if err != nil {
		log.Fatal(err)
	}

	handled := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- w.Run(context.Background(), func(_ context.Context, m *client.Delivered) error {
			fmt.Printf("%s %s\n", m.DedupKey(), m.Body) // DedupKey = stream/seq, stable forever
			handled <- struct{}{}
			return nil // nil ⇒ acked; Permanent ⇒ DLQ; anything else ⇒ nak+retry
		})
	}()

	<-handled
	if drainErr := w.Drain(context.Background()); drainErr != nil { // settle, nak what's held, stop
		log.Fatal(drainErr)
	}
	if runErr := <-done; runErr != nil {
		log.Fatal(runErr)
	}
	// Output: orders/1 hello messq
}

// The messq CLI renders this package: every command is the client call it
// makes, so what you type in a shell has a one-line Go twin.
func Example_shellParity() {
	for _, row := range [][2]string{
		{"messq publish orders.west --body-file data.bin", "client.Publish / PublishReader"},
		{"messq consume orders w", "Client.NewWorker + Worker.Run"},
		{"messq stream create orders", "client.CreateStream"},
		{"messq peek orders 7", "client.PeekMessage / client.PeekMessageData"},
		{"messq consumer edit orders w --ack-wait 60s", "client.UpdateConsumer"},
		{"messq ack orders/w/7/1/1", "client.Ack"},
	} {
		fmt.Println(row[0], "=>", row[1])
	}
	// Output:
	// messq publish orders.west --body-file data.bin => client.Publish / PublishReader
	// messq consume orders w => Client.NewWorker + Worker.Run
	// messq stream create orders => client.CreateStream
	// messq peek orders 7 => client.PeekMessage / client.PeekMessageData
	// messq consumer edit orders w --ack-wait 60s => client.UpdateConsumer
	// messq ack orders/w/7/1/1 => client.Ack
}
