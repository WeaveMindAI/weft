// Compatibility re-export. When the graph webview was extracted from the VS
// Code extension into this shared package, the protocol moved to the package
// root (`../protocol`). The webview's many files import it via the original
// `.../shared/protocol` path; this barrel keeps those imports resolving with no
// per-file churn. The single source of truth is `../protocol`.
export * from '../protocol';
