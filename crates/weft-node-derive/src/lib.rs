//! `#[derive(NodeManifest)]`: implements `weft_core::NodeManifest` from the
//! `metadata.json` sitting next to the deriving type's source file.
//!
//! The derive locates the invoking source file through the compiler
//! (`Span::local_file`), embeds the sibling `metadata.json` via
//! `include_str!` (so editing the json rebuilds the crate), and validates
//! at compile time that the file exists, is a JSON object, and carries a
//! string `type` field. A missing or malformed manifest is a compile
//! error naming the file, not a runtime surprise.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(NodeManifest)]
pub fn derive_node_manifest(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let (member_path, package_defaults_path) = match manifest_paths() {
        Ok(p) => p,
        Err(msg) => return compile_error(&msg),
    };
    if let Err(msg) = validate_manifest(&member_path) {
        return compile_error(&msg);
    }
    let member_str = member_path.to_str().expect("manifest path is valid UTF-8").to_string();
    let site = format!("{ident} ({member_str})");

    // Embed the package root's partial defaults too, when this node is a
    // package member, so `manifest()` is the SAME merged document the catalog
    // builds (formFieldSpecs, ... declared once at the package root
    // reach the runtime metadata). A bare node has no package root, so the
    // derive embeds `None` and there is nothing to merge.
    let defaults_expr = match package_defaults_path {
        Some(path) => {
            if let Err(msg) = validate_defaults(&path) {
                return compile_error(&msg);
            }
            let path_str = path.to_str().expect("package metadata path is valid UTF-8").to_string();
            quote! { Some(include_str!(#path_str)) }
        }
        None => quote! { None },
    };

    quote! {
        impl #impl_generics ::weft_core::NodeManifest for #ident #ty_generics #where_clause {
            fn manifest(&self) -> &'static ::weft_core::NodeMetadata {
                static MANIFEST: ::std::sync::OnceLock<::weft_core::NodeMetadata> =
                    ::std::sync::OnceLock::new();
                MANIFEST.get_or_init(|| {
                    ::weft_core::NodeMetadata::parse_embedded(
                        include_str!(#member_str),
                        #defaults_expr,
                        #site,
                    )
                })
            }
        }
    }
    .into()
}

/// The member's own `metadata.json` and, when the node is a package member,
/// the package root's partial `metadata.json` of shared defaults.
///
/// The package-member rule mirrors the catalog's discovery
/// (`weft-catalog::register_package`): the package root is the member dir's
/// PARENT, and it is a package root iff it holds a `package.toml`. A bare
/// node's parent has no `package.toml`, so it inherits nothing. A package
/// root without a `metadata.json` (no shared defaults) also yields `None`.
fn manifest_paths() -> Result<(std::path::PathBuf, Option<std::path::PathBuf>), String> {
    let source = proc_macro::Span::call_site()
        .local_file()
        .ok_or("derive(NodeManifest): the compiler did not expose the invoking source file")?;
    let dir = source
        .parent()
        .ok_or("derive(NodeManifest): invoking source file has no parent directory")?;
    // No-follow, like the catalog: a symlinked member metadata.json is not part
    // of a unit (the staging copy and the hash walk both drop it), so the derive
    // must not resolve one either, or the runtime node would exist while the
    // catalog skips it.
    let member_path = dir.join("metadata.json");
    if !is_real_file(&member_path) {
        return Err(format!(
            "derive(NodeManifest): no metadata.json next to {} (a symlink does not count). \
             Every node ships a real metadata.json in its own directory.",
            source.display()
        ));
    }
    let member = std::fs::canonicalize(&member_path).map_err(|e| {
        format!("derive(NodeManifest): cannot resolve {}: {e}", member_path.display())
    })?;

    let package_defaults = dir
        .parent()
        .filter(|parent| is_real_file(&parent.join("package.toml")))
        .map(|parent| parent.join("metadata.json"))
        .filter(|defaults| is_real_file(defaults))
        .map(|defaults| {
            std::fs::canonicalize(&defaults).map_err(|e| {
                format!(
                    "derive(NodeManifest): cannot resolve package metadata.json at {}: {e}",
                    defaults.display()
                )
            })
        })
        .transpose()?;

    Ok((member, package_defaults))
}

/// A real (non-symlink) file at `path`. The catalog decides a node's shape from
/// a NO-FOLLOW view of the tree (a symlinked file is not part of a unit, since
/// the staging copy and the hash walk both drop it), so the derive must see the
/// same thing: a followed symlink here would make the runtime metadata and the
/// catalog's disagree about what the package declares.
fn is_real_file(path: &std::path::Path) -> bool {
    std::fs::symlink_metadata(path).map(|m| m.is_file()).unwrap_or(false)
}

/// Keys a package root's partial `metadata.json` may never supply as a default
/// (they are one node's identity, not something a package shares). Restated
/// here because depending on weft-core from a proc-macro crate that weft-core
/// re-exports would be circular; the two must stay in lockstep.
// SYNC: NON_INHERITABLE_KEYS <-> crates/weft-core/src/node.rs NON_INHERITABLE_METADATA_KEYS
const NON_INHERITABLE_KEYS: [&str; 3] = ["type", "label", "description"];

/// Compile-time shape check for a PACKAGE ROOT's partial `metadata.json` (the
/// shared defaults every member inherits). It is embedded and merged into each
/// member's metadata, so a broken one would otherwise only blow up as a runtime
/// panic in a deployed worker. Checked here so it is a COMPILE error naming the
/// package file itself.
///
/// It is PARTIAL by design (it has no `type`, that is the point), so this
/// checks only what it must be: valid JSON, an object, and carrying no identity
/// key. The full `NodeMetadata` shape of the MERGED document is checked by the
/// catalog at build time (and, as a backstop, by `parse_embedded`).
fn validate_defaults(path: &std::path::Path) -> Result<(), String> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        format!("derive(NodeManifest): cannot read package metadata {}: {e}", path.display())
    })?;
    let value: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
        format!(
            "derive(NodeManifest): package metadata {} is not valid JSON: {e}",
            path.display()
        )
    })?;
    let object = value.as_object().ok_or_else(|| {
        format!(
            "derive(NodeManifest): package metadata {} must be a JSON object",
            path.display()
        )
    })?;
    for key in NON_INHERITABLE_KEYS {
        if object.contains_key(key) {
            return Err(format!(
                "derive(NodeManifest): package metadata {} must not set `{key}`: it is one \
                 node's identity, not a package default",
                path.display()
            ));
        }
    }
    Ok(())
}

/// Compile-time shape check: a JSON object with a string `type` field.
/// The full `NodeMetadata` parse happens in weft-core (depending on it
/// here would be circular), so this catches the gross mistakes early.
fn validate_manifest(path: &std::path::Path) -> Result<(), String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("derive(NodeManifest): cannot read {}: {e}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| format!("derive(NodeManifest): {} is not valid JSON: {e}", path.display()))?;
    let object = value
        .as_object()
        .ok_or_else(|| format!("derive(NodeManifest): {} must be a JSON object", path.display()))?;
    match object.get("type") {
        Some(serde_json::Value::String(_)) => Ok(()),
        _ => Err(format!(
            "derive(NodeManifest): {} must carry a string \"type\" field (the node type)",
            path.display()
        )),
    }
}

fn compile_error(msg: &str) -> TokenStream {
    quote! { compile_error!(#msg); }.into()
}
