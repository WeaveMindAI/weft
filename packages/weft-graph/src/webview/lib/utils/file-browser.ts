// Helpers for the file-drop field + its project-file picker: mime guessing
// from a filename (for filtering + marker-kind display), the accept filter
// derived from the field's declared weft file type, and accept matching.
//
// The field's config value is an `@asset("<path-or-url>", <Type>)` ref
// (in memory: the `WeftFileRefValue` the projection round-trips); the
// pre-build asset sync + compile turn it into the stored-file value nodes
// consume. Nothing here talks to storage.

/// Extension -> mime guess (display + filtering only; the worker's fetch/read
/// reports the real Content-Type at run time).
// SYNC: EXT_MIME <-> crates/weft-core/src/storage/mod.rs mime_from_filename
const EXT_MIME: Record<string, string> = {
  png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', webp: 'image/webp',
  gif: 'image/gif', svg: 'image/svg+xml', avif: 'image/avif',
  mp4: 'video/mp4', mov: 'video/quicktime', webm: 'video/webm', mkv: 'video/x-matroska',
  mp3: 'audio/mpeg', wav: 'audio/wav', ogg: 'audio/ogg', flac: 'audio/flac', m4a: 'audio/mp4',
  pdf: 'application/pdf', csv: 'text/csv', txt: 'text/plain', json: 'application/json',
  zip: 'application/zip',
};

export function guessMime(filename: string): string {
  const ext = filename.split('.').pop()?.toLowerCase() ?? '';
  return EXT_MIME[ext] ?? 'application/octet-stream';
}

/// The HTML-accept filter a declared weft file type implies. An explicit
/// `accept` on the field NARROWS this; `File`/`Blob`/unknown accept anything.
// SYNC: acceptForFileType <-> crates/weft-core/src/node.rs Widget::FileDrop (file_type semantics)
export function acceptForFileType(fileType: string | undefined, explicit: string | undefined): string | undefined {
  if (explicit) return explicit;
  switch (fileType) {
    case 'Image': return 'image/*';
    case 'Audio': return 'audio/*';
    case 'Video': return 'video/*';
    default: return undefined;
  }
}

/// Does `mime` satisfy an HTML-accept-style filter (`image/*`, exact mimes,
/// comma lists)? No filter accepts everything.
export function matchesAccept(mime: string, accept: string | undefined): boolean {
  if (!accept) return true;
  return accept.split(',').map((s) => s.trim()).filter(Boolean).some((rule) => {
    if (rule.endsWith('/*')) return mime.startsWith(rule.slice(0, -1));
    return mime === rule;
  });
}
