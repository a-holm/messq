// SPDX-License-Identifier: Apache-2.0

// Package prometheus stands in for the metrics client, which is not a dependency yet. The
// forbidigo rule matches on the package name, so this stub is a faithful instance of what the
// rule forbids and costs nothing from the dependency budget.
package prometheus

// Collector stands in for the client library's Collector interface.
type Collector interface {
	Describe()
}

// MustRegister stands in for registration against the default registry.
func MustRegister(_ ...Collector) {}
