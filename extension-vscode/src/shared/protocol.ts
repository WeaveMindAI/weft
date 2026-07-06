// Re-export of the graph protocol, which now lives in the shared `weft-graph`
// package (so any host can share one graph renderer). The extension host's
// TypeScript imports `./shared/protocol`; this shim keeps those imports working.
// Single source of truth: the package.
export * from '../../../packages/weft-graph/src/protocol';
// The shared status remap (snake_case payload -> ActionAvailability ->
// BackendSnapshot) so every host builds its snapshot through the exact
// same code.
export * from '../../../packages/weft-graph/src/status';
