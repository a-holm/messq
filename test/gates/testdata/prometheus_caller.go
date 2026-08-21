// SPDX-License-Identifier: Apache-2.0

package api

import "github.com/a-holm/messq/internal/sabotageprom/prometheus"

// SabotageRegister registers a collector against the default registry.
func SabotageRegister(c prometheus.Collector) {
	prometheus.MustRegister(c)
}
