// SPDX-License-Identifier: Apache-2.0

// Package obs holds the event vocabulary, the slog handlers (json and human) and the metrics
// seams. The Prometheus instruments live in the prommetrics subpackage: package-granular
// imports would otherwise drag the metrics client — and through it net/http — into every
// importer of obs, including internal/store, which layers.sh keeps below the network layer.
package obs
