// Which stored runtime files the "pick a stored file" picker offers. A
// picked file is written into source as its tenant-less key
// (`@asset("<scope-key>", <Type>)`), the build looks it up in the tenant's
// listing, and at run time the project's worker reads it. Any run of any
// project of the tenant reads any of the tenant's files (a scope says how
// long a file lives, never who reads it), so the picker offers every file
// of every scope:
//   `run/<run>/<id>`                       a run's, gone with the run's record
//   `project/<project>/<id>`               a project's, gone with the project
//   `instance/<project>/<instance>/<id>`   an instance's
//   `shared/<name>/<id>`                   a shared space's
// Left out: `asset/<sha256>`. An asset is the store's copy of a file some
// source names by its path (every file of every version snapshot, of every
// project of the tenant, plus each media `@asset`), so listing them floods
// the picker with source files, and a field reaches one through the path it
// already has on disk.
// SYNC: storedFilePickerRows <-> crates/weft-core/src/storage/key.rs check_key_access

export type StoredFileRow = { key: string; filename: string; mimeType: string; sizeBytes: number };

/** The rows of `files` (the dispatcher's tenant listing, keys
 *  `<tenant>/<scope-key>`) a field can take as its value, each with its
 *  key made tenant-less. */
export function storedFilePickerRows(files: StoredFileRow[]): StoredFileRow[] {
  return files.flatMap((f) => {
    const slash = f.key.indexOf('/');
    if (slash < 0) return [];
    const scopeKey = f.key.slice(slash + 1);
    if (!pickable(scopeKey)) return [];
    return [{ key: scopeKey, filename: f.filename, mimeType: f.mimeType, sizeBytes: f.sizeBytes }];
  });
}

function pickable(scopeKey: string): boolean {
  const parts = scopeKey.split('/');
  switch (parts[0]) {
    case 'run':
    case 'project':
    case 'shared':
      return parts.length === 3;
    case 'instance':
      return parts.length === 4;
    default:
      return false;
  }
}
