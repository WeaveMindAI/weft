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
    let mut chars = id.chars();
    let valid_first = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_');
    if valid_first
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !crate::weft_compiler::is_reserved_local(&id)
    {
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
