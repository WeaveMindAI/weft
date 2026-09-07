// The Problems-panel bookkeeping, pure over strings so it is testable
// with a hand-rolled sink (no vscode import).
//
// Two axes it keeps straight:
//  - OWNERS: each open document's validation run owns one slice of
//    findings, keyed by the document's URI string (unique per
//    document, so two open documents resolving to the same real file
//    cannot overwrite each other's slice).
//  - FILES: each finding lands on a file, keyed by canonical path so
//    every spelling of the file meets on one bucket. What the panel
//    shows for a file is the union of every owner's findings for it,
//    published at EVERY open document of that file (each spelling's
//    editor gets its own squiggles) or, when none is open, at the
//    file's own URI.

/// Where merged findings are published. URIs are strings; the vscode
/// layer parses them back.
export interface DiagSink<T> {
  set(uri: string, diags: T[]): void;
  delete(uri: string): void;
}

export class DiagnosticRouter<T> {
  /// owner uri string -> (canonical file -> findings)
  private readonly findingsByOwner = new Map<string, Map<string, T[]>>();
  /// canonical file -> the uri strings of every open document of it
  private readonly fileUris = new Map<string, Set<string>>();

  constructor(
    private readonly sink: DiagSink<T>,
    /// The URI a file's findings publish at when no document of it is
    /// open (a closed `@include`, typically its file: URI).
    private readonly fallbackUri: (file: string) => string,
  ) {}

  /** A document of `file` opened at `uri`: its editor gets the file's
   *  squiggles from now on. */
  register(uri: string, file: string): void {
    let uris = this.fileUris.get(file);
    if (!uris) {
      uris = new Set();
      this.fileUris.set(file, uris);
    }
    uris.add(uri);
    this.republish(file);
  }

  /** Replace one owner's whole slice, then re-derive the panel entry
   *  of every file either the old or the new slice touches. */
  publish(owner: string, slice: Map<string, T[]>): void {
    const touched = new Set<string>([
      ...(this.findingsByOwner.get(owner)?.keys() ?? []),
      ...slice.keys(),
    ]);
    if (slice.size === 0) this.findingsByOwner.delete(owner);
    else this.findingsByOwner.set(owner, slice);
    for (const file of touched) this.republish(file);
  }

  /** The document at `uri` closed: its slice goes, its editor's entry
   *  goes, and the file's surviving findings move to whatever open
   *  documents (or the fallback URI) remain. The URI is unregistered
   *  FIRST, so no intermediate publish can land on the dying editor. */
  close(uri: string, file: string): void {
    const uris = this.fileUris.get(file);
    const held = uris?.delete(uri) ?? false;
    if (held) {
      if (uris && uris.size === 0) this.fileUris.delete(file);
      this.sink.delete(uri);
    }
    this.publish(uri, new Map());
    if (held) this.republish(file);
  }

  private republish(file: string): void {
    const merged: T[] = [];
    for (const slice of this.findingsByOwner.values()) {
      merged.push(...(slice.get(file) ?? []));
    }
    const fallback = this.fallbackUri(file);
    const uris = this.fileUris.get(file);
    if (!uris || uris.size === 0) {
      if (merged.length > 0) this.sink.set(fallback, merged);
      else this.sink.delete(fallback);
      return;
    }
    // Documents are open: each spelling's editor gets the merged list,
    // and the closed-file fallback entry (from before any of them
    // opened) is cleared unless it IS one of the open spellings.
    if (!uris.has(fallback)) this.sink.delete(fallback);
    for (const uri of uris) {
      if (merged.length > 0) this.sink.set(uri, merged);
      else this.sink.delete(uri);
    }
  }
}
