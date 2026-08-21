// Package forbidden stands in for a dependency nobody voted for. A local replace keeps the
// sabotage offline: resolving a real module would need the network and a go.sum entry.
package forbidden

// X exists so the module has a package.
func X() {}
