//! Derive a source file's identity (id + display label) from its filename.
//!
//! A `.weft` file declares a top-level group, which is the file's interface. A
//! NAMED group (`MyCleaner = Group() { ... }`) carries its own id. An ANONYMOUS
//! one (`Group() { ... }` with no `=`) takes its id from the FILE'S name,
//! because the file IS its identity (an `@include("my-cleaner.weft")` refers to
//! it by path, never by an internal sentinel). The same derived id is used at
//! parse, edit, and render: there is exactly one name for the file at any time.
//!
//! A file with no usable name (an unsaved editor buffer; a path that has no
//! valid identifier characters) gets `Untitled` as a stable fallback.

use std::path::Path;

/// The id of an included file's body: its path from the project root,
/// extension dropped, folders joined with `:`, behind a leading `@`
/// (`lib/clean.weft` is `@lib:clean`). No program can spell `@` or `:`
/// in a name, so the id clashes with nothing a person writes, and two
/// files with one name in two folders are two bodies. The id holds no
/// `.` (a body's nodes are `<body>.<name>`, split at the first dot) and
/// no `__` (a boundary is `<group>__in`), so any other character in a
/// path becomes `_`; two paths that land on one id are refused by the
/// compiler. It is internal: every surface that talks to a person names
/// the file's nodes through the site that calls them (`one.strip`),
/// never through this. A file outside the root keeps its name alone
/// (`@clean`).
pub fn body_id(root: &Path, file: &Path) -> String {
    let relative = file.strip_prefix(root).unwrap_or_else(|_| Path::new(file.file_name().unwrap_or_default()));
    let mut parts: Vec<String> = relative.components()
        .filter_map(|c| match c { std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()), _ => None })
        .collect();
    if let Some(last) = parts.last_mut() {
        if let Some(stem) = Path::new(last.as_str()).file_stem() { *last = stem.to_string_lossy().into_owned(); }
    }
    // Any run of other characters is one `_`, so no `__` can form.
    let plain = |part: &str| {
        let mut out = String::new();
        for c in part.chars() {
            if c.is_ascii_alphanumeric() { out.push(c); } else if !out.ends_with('_') { out.push('_'); }
        }
        out
    };
    format!("@{}", parts.iter().map(|part| plain(part)).collect::<Vec<_>>().join(":"))
}

/// How a group id reads to a PERSON, which is never the id itself for
/// an included file's body. A body id is the file's path
/// (`@src:lib:clean`), unspellable on purpose, so it shows as the
/// file's own name (`clean`); a nested group's id carries its parents
/// (`outer.inner`), so it shows as its last segment; anything else is
/// already a name a person wrote.
///
/// Every surface that puts a group in front of a person goes through
/// this: the editor's box header, a boundary node's label, anywhere
/// else that would otherwise print the id.
// SYNC: display_name <-> packages/weft-graph/src/webview/host-bridge.ts displayName
pub fn display_name(group_id: &str) -> String {
    let plain = weft_core::project::plain_id(group_id);
    match plain.rsplit_once('.') {
        Some((_, local)) => local.to_string(),
        None => plain,
    }
}

/// The anonymous-root id a file is parsed under: the body id when the
/// file sits in a project (the compiler gives an `@include` of it the
/// same id, so the editor's standalone view of the file and the journal
/// rows agree), else the filename-derived id.
pub fn file_id(root: Option<&Path>, file: Option<&Path>) -> String {
    match (root, file) {
        (Some(root), Some(file)) => body_id(root, file),
        _ => derive_id(file),
    }
}

/// A file's anonymous-root group id, derived from the filename's stem in
/// PascalCase. Falls back to `Untitled` for an unsaved buffer / unusable name.
pub fn derive_id(file: Option<&Path>) -> String {
    file.and_then(|p| p.file_stem()?.to_str().map(pascal_case))
        .flatten()
        .unwrap_or_else(|| "Untitled".to_string())
}

/// A human-readable label for the same file: words from the filename stem,
/// space-separated and capitalized. Falls back to the id when the stem has no
/// usable letters.
pub fn derive_label(file: Option<&Path>) -> String {
    let label = file
        .and_then(|p| p.file_stem().and_then(|s| s.to_str()))
        .map(humanize)
        .unwrap_or_default();
    if label.is_empty() { derive_id(file) } else { label }
}

/// PascalCase the stem (split on `-`/`_`/space, capitalize each word, concatenate).
/// Returns None (so the caller falls back to `Untitled`) if the result isn't a
/// valid weft bare identifier (`[A-Za-z_][\w]*`) OR is a name the language
/// reserves (`Group`/`Passthrough`/`self`/`__`-containing). The reserved check
/// keeps the anonymous-root id (which skips the user-name gate) honest: an id a
/// user couldn't write as a decl name must not sneak in via a filename either.
fn pascal_case(stem: &str) -> Option<String> {
    let id = capitalized_words(stem).join("");
    if weft_core::is_rust_identifier(&id) && !crate::weft_compiler::is_reserved_local(&id) {
        Some(id)
    } else {
        None
    }
}

fn humanize(stem: &str) -> String {
    capitalized_words(stem).join(" ")
}

fn capitalized_words(stem: &str) -> Vec<String> {
    stem.split(|c: char| c == '-' || c == '_' || c.is_whitespace())
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut cs = w.chars();
            match cs.next() {
                Some(c) => c.to_ascii_uppercase().to_string() + cs.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn id(s: &str) -> String { derive_id(Some(&PathBuf::from(s))) }

    #[test]
    fn a_body_id_is_the_path_from_the_root_and_unspellable() {
        let root = PathBuf::from("/p");
        assert_eq!(body_id(&root, &root.join("src/lib/clean.weft")), "@src:lib:clean");
        assert_eq!(body_id(&root, &root.join("clean.weft")), "@clean");
        assert_eq!(body_id(&root, &PathBuf::from("/elsewhere/clean.weft")), "@clean");
        // Neither a dot nor a boundary marker survives into the id.
        assert_eq!(body_id(&root, &root.join("src/v1.2/clean.gen.weft")), "@src:v1_2:clean_gen");
        assert_eq!(body_id(&root, &root.join("a__in.weft")), "@a_in");
        assert_eq!(body_id(&root, &root.join("a---in.weft")), "@a_in");
        assert!(!weft_core::is_rust_identifier("@src:lib:clean"));
        assert_eq!(file_id(None, Some(&PathBuf::from("my-cleaner.weft"))), "MyCleaner");
    }
    /// A body id is a path, so it reads as the file; a nested group
    /// reads as its own segment; a plain name is already one.
    #[test]
    fn a_group_reads_to_a_person_as_its_own_name_never_its_path() {
        assert_eq!(display_name("@src:lib:clean"), "clean");
        assert_eq!(display_name("@clean"), "clean");
        assert_eq!(display_name("outer.inner"), "inner");
        assert_eq!(display_name("cards"), "cards");
        // A body's nested group carries both shapes at once.
        assert_eq!(display_name("@src:cards.rows"), "rows");
    }

    fn label(s: &str) -> String { derive_label(Some(&PathBuf::from(s))) }

    #[test]
    fn normal_filenames() {
        assert_eq!(id("/x/my-cleaner.weft"), "MyCleaner");
        assert_eq!(label("/x/my-cleaner.weft"), "My Cleaner");
        assert_eq!(id("/x/cleaner.weft"), "Cleaner");
        assert_eq!(id("/x/my_cleaner.weft"), "MyCleaner");
    }

    #[test]
    fn unsaved_buffer_falls_back() {
        assert_eq!(derive_id(None), "Untitled");
        assert_eq!(derive_label(None), "Untitled");
    }

    #[test]
    fn invalid_identifier_falls_back() {
        // a leading digit makes the pascal-cased id not a valid bare ident
        assert_eq!(id("/x/123-bad.weft"), "Untitled");
    }

    #[test]
    fn reserved_id_falls_back() {
        // A filename whose PascalCased id is a RESERVED language name must NOT
        // become the anonymous-root id (the anon path skips the user-name gate,
        // so the derivation itself must refuse a reserved id). Falls back to
        // `Untitled`, the same as any unusable name.
        assert_eq!(id("/x/Group.weft"), "Untitled");
        assert_eq!(id("/x/Passthrough.weft"), "Untitled");
        // `self.weft` PascalCases to `Self` (capitalized away from the lowercase
        // `self` keyword), which is NOT reserved, so it's allowed.
        assert_eq!(id("/x/self.weft"), "Self");
        // `__` in the stem is consumed by the word-splitter, so it can't survive
        // into the id; a `my__weird` file is a clean `MyWeird`.
        assert_eq!(id("/x/my__weird.weft"), "MyWeird");
    }
}
