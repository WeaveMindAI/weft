#!/usr/bin/env bash
set -euo pipefail

# Requires Bash 4+ for associative arrays (declare -A).
# macOS ships Bash 3.2. Install Bash 4+ via: brew install bash
if ((BASH_VERSINFO[0] < 4)); then
    echo "ERROR: Bash 4+ required (you have $BASH_VERSION)."
    echo "  macOS: brew install bash"
    echo "  Then run: /opt/homebrew/bin/bash $0 $*"
    exit 1
fi

# catalog-link.sh, Links catalog/ files into the Rust, TS, and sidecar directories.
#
# Usage:
#   bash catalog-link.sh           # symlink mode (default, local dev)
#   bash catalog-link.sh --copy    # copy mode (CI/CD, Docker builds)
#
# Naming convention:
#   - Folders prefixed with "_" contribute their name (without "_") as a prefix.
#   - The leaf folder name is the suffix.
#   - Joined with "-" to form the canonical node name (kebab-case).
#   - Example: catalog/communication/_discord/send/ → "discord-send"
#
# Files handled:
#   - backend.rs  → linked into crates/weft-nodes/src/nodes/{snake_case}/mod.rs
#   - frontend.ts → linked into dashboard/src/lib/nodes/{kebab-case}.ts
#   - lib.rs      → linked as {prefix}_lib/mod.rs (Rust) using the parent _prefix folder name
#   - sidecar/    → linked as sidecars/{resolved-name}/ (directory)
#   - Extra files (e.g. .py) in catalog node dirs → linked into the Rust node folder
#
# Also generates catalog-tree.json for the AI builder.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CATALOG_DIR="$ROOT_DIR/catalog"
RUST_NODES_DIR="$ROOT_DIR/crates/weft-nodes/src/nodes"
TS_NODES_DIR="$ROOT_DIR/dashboard/src/lib/nodes"
SIDECARS_DIR="$ROOT_DIR/sidecars"

# --- Mode: symlink (default) or copy (--copy) ---
LINK_MODE="symlink"
if [[ "${1:-}" == "--copy" ]]; then
    LINK_MODE="copy"
fi

# --- Collect existing generated entries for reconciliation ---
# Instead of deleting everything and recreating (which causes flickering),
# we track what exists and what we generate, then only remove stale entries.
declare -A EXISTING_RUST EXISTING_TS EXISTING_SIDECAR
declare -A WANTED_RUST WANTED_TS WANTED_SIDECAR

# Snapshot current generated node directories (symlinks and copies, excluding hand-written files)
for d in "$RUST_NODES_DIR"/*/; do
    [[ -e "$d" || -L "${d%/}" ]] || continue
    dname="$(basename "$d")"
    EXISTING_RUST["$dname"]=1
done
for f in "$TS_NODES_DIR"/*.ts; do
    # -e || -L: regular files OR symlinks (including broken ones, which need
    # to be reconciled when their target catalog dir is renamed/removed).
    [[ -e "$f" || -L "$f" ]] || continue
    bname="$(basename "$f")"
    [[ "$bname" == "index.ts" ]] && continue
    EXISTING_TS["$bname"]=1
done
for d in "$SIDECARS_DIR"/*/; do
    [[ -e "$d" ]] || continue
    dname="$(basename "$d")"
    [[ "$dname" == "examples" ]] && continue
    EXISTING_SIDECAR["$dname"]=1
done

# Helper: link or copy a file (overwrites existing).
#
# Defense-in-depth against stale state from a previously aborted run:
#   - `rm -f` clears a regular file or symlink at the target path.
#   - `ln -sf` atomically replaces anything still at the target.
# Either step alone is enough in the happy path, but combining them
# means the script tolerates weird filesystem states (leftover
# partial runs, permission hiccups, filesystems where rm reported
# success but didn't actually remove the entry) without bailing out
# with "File exists" and requiring a manual `rm -rf nodes/`.
link_file() {
    local source="$1"
    local target="$2"
    rm -f "$target"
    if [[ "$LINK_MODE" == "copy" ]]; then
        cp -f "$source" "$target"
    else
        local rel="$(portable_relpath "$source" "$(dirname "$target")")"
        ln -sf "$rel" "$target"
    fi
}

# Helper: link or copy a directory (overwrites existing).
#
# Uses `ln -sfn` for the symlink case (`-f` forces replace, `-n`
# prevents GNU ln from following an existing symlinked directory
# and creating a nested symlink inside it — the classic
# `ln -sf new /path/to/existing_symlink_dir/new` → nests bug).
link_dir() {
    local source="$1"
    local target="$2"
    # Remove existing (symlink or copied dir)
    if [[ -L "$target" ]]; then
        rm -f "$target"
    elif [[ -d "$target" ]]; then
        rm -rf "$target"
    fi
    if [[ "$LINK_MODE" == "copy" ]]; then
        cp -rf "$source" "$target"
    else
        local rel="$(portable_relpath "$source" "$(dirname "$target")")"
        ln -sfn "$rel" "$target"
    fi
}

# --- Tracking for uniqueness validation ---
declare -A SEEN_RUST_NAMES
declare -A SEEN_TS_NAMES
ERRORS=()

# Portable relative path (works on macOS without GNU coreutils)
portable_relpath() {
    local source="$1"
    local target_dir="$2"
    if command -v realpath &>/dev/null && realpath --relative-to=/ / &>/dev/null 2>&1; then
        realpath --relative-to="$target_dir" "$source"
    else
        # Python fallback for macOS
        python3 -c "import os; print(os.path.relpath('$source', '$target_dir'))"
    fi
}

to_snake_case() {
    echo "$1" | sed 's/-/_/g'
}

# --- Walk the catalog and resolve names ---
# For each file, we walk up from its directory to catalog/ collecting _prefix segments.
resolve_name() {
    local file_path="$1"
    local dir="$(dirname "$file_path")"
    local filename="$(basename "$file_path")"

    # Collect path segments between catalog/ and the file
    local rel_path="${dir#$CATALOG_DIR/}"
    local IFS='/'
    read -ra segments <<< "$rel_path"

    local prefixes=()
    local leaf=""

    for seg in "${segments[@]}"; do
        if [[ "$seg" == _* ]]; then
            # This is a prefix folder, strip the "_" and accumulate
            prefixes+=("${seg#_}")
        else
            # Non-prefix folder, this could be the leaf or just organizational
            leaf="$seg"
        fi
    done

    if [[ "$filename" == "lib.rs" ]]; then
        # lib.rs files: name = last_prefix + "_lib"
        # e.g., _email/lib.rs → email_lib
        if [[ ${#prefixes[@]} -gt 0 ]]; then
            local last_prefix="${prefixes[-1]}"
            echo "$(to_snake_case "$last_prefix")_lib"
        else
            echo "lib"
        fi
    elif [[ "$filename" == "backend.rs" || "$filename" == "frontend.ts" ]]; then
        # Node files: name = prefixes joined with "-" + leaf
        local parts=("${prefixes[@]}" "$leaf")
        local name=""
        for p in "${parts[@]}"; do
            if [[ -n "$name" ]]; then
                name="$name-$p"
            else
                name="$p"
            fi
        done
        echo "$name"
    fi
}

RUST_LINKS=0
TS_LINKS=0
SIDECAR_LINKS=0
declare -A SEEN_SIDECAR_NAMES

# --- Process backend.rs files ---
while IFS= read -r -d '' file || [[ -n "$file" ]]; do
    name="$(resolve_name "$file")"
    snake="$(to_snake_case "$name")"
    node_dir="$RUST_NODES_DIR/${snake}"
    target="$node_dir/mod.rs"

    if [[ -n "${SEEN_RUST_NAMES[$snake]+x}" ]]; then
        ERRORS+=("DUPLICATE Rust name '$snake': $file conflicts with ${SEEN_RUST_NAMES[$snake]}")
        continue
    fi
    SEEN_RUST_NAMES["$snake"]="$file"

    WANTED_RUST["$snake"]=1
    mkdir -p "$node_dir"
    link_file "$file" "$target"
    RUST_LINKS=$((RUST_LINKS + 1))

    # Link extra files and directories from the catalog node directory into the Rust node folder
    catalog_node_dir="$(dirname "$file")"
    for extra in "$catalog_node_dir"/*; do
        extra_name="$(basename "$extra")"
        # Skip files already handled
        [[ "$extra_name" == "backend.rs" || "$extra_name" == "frontend.ts" || "$extra_name" == "lib.rs" ]] && continue
        # Skip sidecar dirs (handled separately)
        [[ "$extra_name" == "sidecar" ]] && continue
        if [[ -d "$extra" ]]; then
            link_dir "$extra" "$node_dir/$extra_name"
        elif [[ -f "$extra" ]]; then
            link_file "$extra" "$node_dir/$extra_name"
        fi
    done
done < <(find -L "$CATALOG_DIR" -name "backend.rs" -print0)

# --- Process lib.rs files ---
while IFS= read -r -d '' file || [[ -n "$file" ]]; do
    name="$(resolve_name "$file")"
    lib_dir="$RUST_NODES_DIR/${name}"
    target="$lib_dir/mod.rs"

    if [[ -n "${SEEN_RUST_NAMES[$name]+x}" ]]; then
        ERRORS+=("DUPLICATE Rust name '$name': $file conflicts with ${SEEN_RUST_NAMES[$name]}")
        continue
    fi
    SEEN_RUST_NAMES["$name"]="$file"

    WANTED_RUST["$name"]=1
    mkdir -p "$lib_dir"
    link_file "$file" "$target"
    RUST_LINKS=$((RUST_LINKS + 1))
done < <(find -L "$CATALOG_DIR" -name "lib.rs" -print0)

# --- Process frontend.ts files ---
while IFS= read -r -d '' file || [[ -n "$file" ]]; do
    name="$(resolve_name "$file")"
    target="$TS_NODES_DIR/${name}.ts"

    if [[ -n "${SEEN_TS_NAMES[$name]+x}" ]]; then
        ERRORS+=("DUPLICATE TS name '$name': $file conflicts with ${SEEN_TS_NAMES[$name]}")
        continue
    fi
    SEEN_TS_NAMES["$name"]="$file"

    WANTED_TS["${name}.ts"]=1
    link_file "$file" "$target"
    TS_LINKS=$((TS_LINKS + 1))
done < <(find -L "$CATALOG_DIR" -name "frontend.ts" -print0)

# --- Process sidecar/ directories ---
# A sidecar/ dir inside a catalog node folder gets linked into sidecars/ with the resolved name.
# e.g., catalog/communication/:whatsapp/bridge/sidecar/ → sidecars/whatsapp-bridge
# e.g., catalog/storage/:postgres/database/sidecar/ → sidecars/postgres-database
while IFS= read -r -d '' sidecar_dir || [[ -n "$sidecar_dir" ]]; do
    # The parent of sidecar/ is the node dir, resolve name from a fake file in the parent
    parent_dir="$(dirname "$sidecar_dir")"
    name="$(resolve_name "$parent_dir/backend.rs")"
    target="$SIDECARS_DIR/$name"

    if [[ -n "${SEEN_SIDECAR_NAMES[$name]+x}" ]]; then
        ERRORS+=("DUPLICATE sidecar name '$name': $sidecar_dir conflicts with ${SEEN_SIDECAR_NAMES[$name]}")
        continue
    fi
    SEEN_SIDECAR_NAMES["$name"]="$sidecar_dir"

    WANTED_SIDECAR["$name"]=1
    link_dir "$sidecar_dir" "$target"
    SIDECAR_LINKS=$((SIDECAR_LINKS + 1))
done < <(find -L "$CATALOG_DIR" -type d -name "sidecar" -print0)

# --- Report ---
if [[ ${#ERRORS[@]} -gt 0 ]]; then
    echo "ERROR: Name conflicts detected!"
    for err in "${ERRORS[@]}"; do
        echo "  $err"
    done
    exit 1
fi

# --- Remove stale entries (exist on disk but not wanted by current catalog) ---
STALE=0
for dname in "${!EXISTING_RUST[@]}"; do
    if [[ -z "${WANTED_RUST[$dname]+x}" ]]; then
        rm -rf "$RUST_NODES_DIR/$dname"
        STALE=$((STALE + 1))
    fi
done
for bname in "${!EXISTING_TS[@]}"; do
    if [[ -z "${WANTED_TS[$bname]+x}" ]]; then
        rm -f "$TS_NODES_DIR/$bname"
        STALE=$((STALE + 1))
    fi
done
for dname in "${!EXISTING_SIDECAR[@]}"; do
    if [[ -z "${WANTED_SIDECAR[$dname]+x}" ]]; then
        target="$SIDECARS_DIR/$dname"
        if [[ -L "$target" ]]; then
            rm -f "$target"
        elif [[ -d "$target" ]]; then
            rm -rf "$target"
        fi
        STALE=$((STALE + 1))
    fi
done

echo "catalog-link: ${RUST_LINKS} Rust, ${TS_LINKS} TS, ${SIDECAR_LINKS} sidecar (mode: ${LINK_MODE})${STALE:+, removed ${STALE} stale}."

# --- Generate nodes/mod.rs ---
# automod::dir! doesn't support folder-based modules (foo/mod.rs), so we generate
# the mod.rs file with explicit pub mod declarations for each discovered node.
{
    echo "// Auto-generated by catalog-link.sh, do not edit manually."
    echo "// Each node module is a folder with mod.rs linked from the catalog."
    for name in $(echo "${!SEEN_RUST_NAMES[@]}" | tr ' ' '\n' | sort); do
        echo "pub mod ${name};"
    done
} > "$RUST_NODES_DIR/mod.rs"
echo "catalog-link: nodes/mod.rs generated (${#SEEN_RUST_NAMES[@]} modules)."

# --- Generate catalog-tree.json ---
# Walks catalog/ and builds a JSON tree of categories and nodes.
generate_catalog_tree() {
    python3 -c "
import os, json, sys

catalog_dir = sys.argv[1]

def walk_tree(path, rel=''):
    entries = sorted(os.listdir(path))
    result = {}
    for entry in entries:
        full = os.path.join(path, entry)
        if not os.path.isdir(full):
            continue
        # Skip non-category dirs
        if entry in ('examples',):
            continue

        display_name = entry.lstrip('_')
        is_prefix = entry.startswith('_')
        child_rel = f'{rel}/{display_name}' if rel else display_name

        # Check if this is a leaf node (has backend.rs or frontend.ts)
        has_backend = os.path.isfile(os.path.join(full, 'backend.rs'))
        has_frontend = os.path.isfile(os.path.join(full, 'frontend.ts'))
        has_sidecar = os.path.isdir(os.path.join(full, 'sidecar'))

        if has_backend or has_frontend:
            # This is a node leaf
            node_info = {
                'type': 'node',
                'hasBackend': has_backend,
                'hasFrontend': has_frontend,
            }
            if has_sidecar:
                node_info['hasSidecar'] = True
            result[display_name] = node_info
        else:
            # This is a category, recurse
            children = walk_tree(full, child_rel)
            if children:
                result[display_name] = {
                    'type': 'category',
                    'isPrefix': is_prefix,
                    'children': children,
                }
    return result

tree = walk_tree(catalog_dir)
print(json.dumps(tree, indent=2))
" "$1"
}

generate_catalog_tree "$CATALOG_DIR" > "$CATALOG_DIR/catalog-tree.json"
echo "catalog-link: catalog-tree.json generated."
