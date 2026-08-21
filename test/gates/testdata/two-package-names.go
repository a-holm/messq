// SPDX-License-Identifier: Apache-2.0

package notclient

// SabotageTwoNames gives pkg/client a second package name, which stops go list from computing
// an import graph for the directory at all.
func SabotageTwoNames() {}
