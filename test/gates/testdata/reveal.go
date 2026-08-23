// SPDX-License-Identifier: Apache-2.0

package api

import "github.com/a-holm/messq/internal/auth"

// SabotageReveal reveals a secret from the API layer. internal/api may import internal/auth,
// but it must never call Secret.Reveal; the forbidigo rule is what catches this.
func SabotageReveal() string {
	return auth.Secret("msq1_x_SABOTAGE-CANARY").Reveal()
}
