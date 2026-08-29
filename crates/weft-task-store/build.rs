//! Turn this crate's `migrations/` directory into a table it can read.
//!
//! A migration is a file. Dropping one in `migrations/<group>/` is the whole
//! act of writing it: nothing lists it, nothing declares it, and no Rust
//! changes. This script walks the directory at build time and embeds what it
//! finds, so a forgotten registration cannot happen.
use std::fmt::Write;

fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    println!("cargo:rerun-if-changed={}", root.display());

    let mut groups: Vec<_> = std::fs::read_dir(&root)
        .unwrap_or_else(|e| panic!("read {}: {e}", root.display()))
        .map(|e| e.expect("read a migrations entry").path())
        .filter(|p| p.is_dir())
        .collect();
    groups.sort();

    let mut origins = String::from("static ORIGINS: &[(&str, &str)] = &[\n");
    let mut out = String::from("pub static MIGRATIONS: &[Migration] = &[\n");
    for dir in groups {
        let group = dir.file_name().expect("a named directory").to_string_lossy().to_string();
        println!("cargo:rerun-if-changed={}", dir.display());
        // Released migrations sit in the group directory. Drafts sit one
        // level down, are gitignored, and exist only on the machine that
        // wrote them: they keep a work-in-progress database in step with a
        // shape still being decided, and get collapsed into one released
        // file when it is.
        let drafts = dir.join("drafts");
        if drafts.is_dir() {
            println!("cargo:rerun-if-changed={}", drafts.display());
        }
        let mut files: Vec<_> = [dir.clone(), drafts]
            .iter()
            .filter(|d| d.is_dir())
            .flat_map(|d| {
                std::fs::read_dir(d)
                    .unwrap_or_else(|e| panic!("read {}: {e}", d.display()))
                    .map(|e| e.expect("read a migration file").path())
            })
            .filter(|p| {
                p.extension().is_some_and(|x| x == "sql")
                    && p.file_stem().is_some_and(|s| s != "origin")
            })
            .collect();
        let origin = dir.join("origin.sql");
        if origin.is_file() {
            writeln!(origins, "    ({group:?}, include_str!({:?})),", origin.display().to_string())
                .expect("write to a String");
        }
        files.sort_by_key(|p| p.file_stem().map(|s| s.to_owned()));
        // A released file and a same-stem draft would race each other at
        // boot (the applied-ids table keys on (group, id)); refuse at
        // build time, naming both.
        let mut seen_ids: std::collections::HashMap<String, std::path::PathBuf> =
            Default::default();
        for path in &files {
            let id = path.file_stem().expect("a named file").to_string_lossy().to_string();
            if let Some(first) = seen_ids.insert(id.clone(), path.clone()) {
                panic!(
                    "migrations/{group}: two files share the id '{id}': {} and {}",
                    first.display(),
                    path.display()
                );
            }
        }
        for path in files {
            let id = path.file_stem().expect("a named file").to_string_lossy().to_string();
            let draft = id.starts_with("draft_");
            writeln!(
                out,
                "    Migration {{ group: {group:?}, id: {id:?}, draft: {draft}, \
                 sql: include_str!({:?}) }},",
                path.display().to_string()
            )
            .expect("write to a String");
        }
    }
    out.push_str("];\n");
    origins.push_str("];\n");
    out.push_str(&origins);

    let dest = std::path::Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR")).join("migrations.rs");
    std::fs::write(&dest, out).unwrap_or_else(|e| panic!("write {}: {e}", dest.display()));
}
