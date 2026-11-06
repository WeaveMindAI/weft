# What every agent's install recipe (agents.json `install`) may call. Weft
# runs a recipe with bash, `set -euo pipefail`, after sourcing this file, to
# put the agent into a folder weft owns, shared by every program and every
# weft install on the machine:
#
#   WEFT_NEED_DIR    the agent's own folder: the recipe installs into it and
#                    nowhere else (no sudo, no shell rc file, no global
#                    package); `bin` in agents.json is relative to it
#   WEFT_NEEDS_ROOT  the folder holding every such folder, and the shared
#                    runtimes `need_node` puts there
#
# The same recipe runs on the person's machine (Linux or macOS) and inside a
# Computer (Linux), so it only relies on what both have: bash, python3 (the
# agent server needs it anyway) and the helpers below, never on curl, xz,
# unzip or a package manager.
# SYSTEM tools a recipe cannot install without sudo are agents.json `system`
# packages, which weft reports missing like any other need.

# The machine: `linux` or `darwin`, and `x64` or `arm64`.
weft_os() {
    case "$(uname -s)" in
        Linux) echo linux ;;
        Darwin) echo darwin ;;
        *) echo "weft installs agents on Linux and macOS only, and this is $(uname -s)" >&2; return 1 ;;
    esac
}
weft_arch() {
    case "$(uname -m)" in
        x86_64|amd64) echo x64 ;;
        aarch64|arm64) echo arm64 ;;
        *) echo "weft installs agents on x86_64 and arm64 only, and this is $(uname -m)" >&2; return 1 ;;
    esac
}

# fetch URL DEST: download URL into the file DEST, failing on any HTTP error.
fetch() {
    python3 - "$1" "$2" <<'PY'
import sys, urllib.request
url, dest = sys.argv[1], sys.argv[2]
req = urllib.request.Request(url, headers={"User-Agent": "weft-agent-install"})
with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as f:
    while chunk := r.read(1 << 20):
        f.write(chunk)
PY
}

# extract ARCHIVE DIR [STRIP]: unpack a .tar.gz, .tar.xz, .tar.bz2, .tgz or
# .zip into DIR, dropping the first STRIP path parts (default 0) and keeping
# file modes (so executables stay executable).
extract() {
    python3 - "$1" "$2" "${3:-0}" <<'PY'
import os, sys, tarfile, zipfile, stat
archive, dest, strip = sys.argv[1], sys.argv[2], int(sys.argv[3])
os.makedirs(dest, exist_ok=True)
def target(name):
    parts = [p for p in name.split("/") if p not in ("", ".")][strip:]
    if not parts or ".." in parts:
        return None
    return os.path.join(dest, *parts)
if zipfile.is_zipfile(archive):
    with zipfile.ZipFile(archive) as z:
        for info in z.infolist():
            path = target(info.filename)
            if path is None:
                continue
            if info.is_dir():
                os.makedirs(path, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with z.open(info) as src, open(path, "wb") as out:
                out.write(src.read())
            mode = (info.external_attr >> 16) & 0o777
            if mode:
                os.chmod(path, mode)
else:
    with tarfile.open(archive) as t:
        for member in t.getmembers():
            path = target(member.name)
            if path is None:
                continue
            if member.isdir():
                os.makedirs(path, exist_ok=True)
            elif member.issym():
                os.makedirs(os.path.dirname(path), exist_ok=True)
                if os.path.lexists(path):
                    os.remove(path)
                os.symlink(member.linkname, path)
            elif member.isfile():
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with t.extractfile(member) as src, open(path, "wb") as out:
                    out.write(src.read())
                os.chmod(path, member.mode & 0o777 | stat.S_IRUSR)
PY
}

# with_lock FILE CMD...: run CMD holding an exclusive lock on FILE, so two
# installs of one shared thing never run at once (flock(1) is not on macOS).
with_lock() {
    local lock="$1"
    shift
    python3 - "$lock" "$@" <<'PY'
import fcntl, os, subprocess, sys
lock, cmd = sys.argv[1], sys.argv[2:]
os.makedirs(os.path.dirname(lock) or ".", exist_ok=True)
with open(lock, "w") as f:
    fcntl.flock(f, fcntl.LOCK_EX)
    sys.exit(subprocess.call(cmd))
PY
}

# need_node MAJOR: print the bin folder of a Node.js MAJOR.x that weft keeps
# in WEFT_NEEDS_ROOT/node-MAJOR, downloading the newest MAJOR.x release from
# nodejs.org the first time any recipe asks for it. Shared by every agent
# that needs that Node, installed once, under a lock.
need_node() {
    local major="$1" dir="$WEFT_NEEDS_ROOT/node-$1"
    if [ ! -x "$dir/bin/node" ]; then
        with_lock "$WEFT_NEEDS_ROOT/node-$major.lock" bash -c '
            set -euo pipefail
            source "$1"
            major="$2" dir="$3"
            [ -x "$dir/bin/node" ] && exit 0
            version=$(python3 -c "
import json, sys, urllib.request
req = urllib.request.Request(\"https://nodejs.org/dist/index.json\", headers={\"User-Agent\": \"weft-agent-install\"})
releases = json.load(urllib.request.urlopen(req, timeout=60))
print(next(r[\"version\"] for r in releases if r[\"version\"].startswith(\"v$major.\")))")
            os=$(weft_os) arch=$(weft_arch)
            tmp=$(mktemp -d)
            fetch "https://nodejs.org/dist/$version/node-$version-$os-$arch.tar.gz" "$tmp/node.tar.gz"
            extract "$tmp/node.tar.gz" "$tmp/node" 1
            rm -rf "$dir" && mv "$tmp/node" "$dir" && rm -rf "$tmp"
        ' _ "${BASH_SOURCE[0]}" "$major" "$dir"
    fi
    echo "$dir/bin"
}

# weft_libc: the C library of a Linux machine, `gnu` or `musl` (for a vendor
# that ships a build for each). Not for macOS.
weft_libc() {
    if ldd --version 2>&1 | grep -qi musl || compgen -G "/lib/ld-musl-*" >/dev/null; then
        echo musl
    else
        echo gnu
    fi
}

# check_sha256 FILE HEX: fail unless FILE's SHA-256 is HEX (sha256sum is not
# on macOS, shasum not on every Linux).
check_sha256() {
    python3 - "$1" "$2" <<'PY'
import hashlib, sys
path, want = sys.argv[1], sys.argv[2].strip().lower()
h = hashlib.sha256()
with open(path, "rb") as f:
    while chunk := f.read(1 << 20):
        h.update(chunk)
if h.hexdigest() != want:
    sys.exit(f"{path}: SHA-256 is {h.hexdigest()}, the vendor says {want}")
PY
}

# json_get FILE EXPR: print the Python expression EXPR, evaluated with `j`
# bound to FILE's JSON (a list of the values when FILE is JSON lines). A
# missing key or no match fails, naming EXPR.
json_get() {
    python3 - "$1" "$2" <<'PY'
import json, sys
path, expr = sys.argv[1], sys.argv[2]
text = open(path).read()
try:
    j = json.loads(text)
except json.JSONDecodeError:
    j = [json.loads(line) for line in text.splitlines() if line.strip()]
try:
    print(eval(expr, {"j": j}))
except (KeyError, IndexError, StopIteration) as e:
    sys.exit(f"{path} has no {expr} ({type(e).__name__}: {e})")
PY
}

# npm_install NODE_MAJOR PACKAGE... [NPM FLAGS]: install npm packages into
# WEFT_NEED_DIR with the shared Node NODE_MAJOR (need_node), npm's cache and
# config kept in the folder. Their programs land in WEFT_NEED_DIR/bin, beside
# a `node` link to that Node so they run without the person's own.
npm_install() {
    local node_bin
    node_bin=$(need_node "$1")
    shift
    mkdir -p "$WEFT_NEED_DIR/bin"
    PATH="$node_bin:$PATH" \
        npm_config_prefix="$WEFT_NEED_DIR" \
        npm_config_cache="$WEFT_NEED_DIR/.npm" \
        npm_config_userconfig="$WEFT_NEED_DIR/.npmrc" \
        npm_config_devdir="$WEFT_NEED_DIR/.node-gyp" \
        npm_config_update_notifier=false npm_config_fund=false npm_config_audit=false \
        "$node_bin/npm" install --global "$@"
    # npm's download cache (90 to 330 MB) is nothing the installed agent
    # reads.
    rm -rf "$WEFT_NEED_DIR/.npm"
    ln -sfn "$node_bin/node" "$WEFT_NEED_DIR/bin/node"
}

# fetch_summed URL SUMS_URL DEST: download URL into the file DEST and fail
# unless its SHA-256 is the one SUMS_URL (a vendor's `<hex>  <file name>`
# list) gives for URL's file name.
fetch_summed() {
    local sums="$3.sums"
    fetch "$1" "$3"
    fetch "$2" "$sums"
    check_sha256 "$3" "$(python3 - "$sums" "${1##*/}" <<'PY'
import sys
sums, name = sys.argv[1], sys.argv[2]
found = [line.split()[0] for line in open(sums) if line.split()[1:] and line.split()[-1].lstrip("*") == name]
if len(found) != 1:
    sys.exit(f"{sums} lists {len(found)} SHA-256 for {name}, where one was expected")
print(found[0])
PY
)"
    rm -f "$sums"
}

# need_uv: print the folder holding uv, which weft keeps in
# WEFT_NEEDS_ROOT/uv, downloading Astral's newest release the first time
# any recipe asks for it. Shared like `need_node`, installed once, under a
# lock. Recipes run it through `weft_uv`.
need_uv() {
    local dir="$WEFT_NEEDS_ROOT/uv"
    if [ ! -x "$dir/uv" ]; then
        with_lock "$WEFT_NEEDS_ROOT/uv.lock" bash -c '
            set -euo pipefail
            source "$1"
            dir="$2"
            [ -x "$dir/uv" ] && exit 0
            case "$(weft_os)-$(weft_arch)" in
                linux-x64) target=x86_64-unknown-linux-musl ;;
                linux-arm64) target=aarch64-unknown-linux-musl ;;
                darwin-x64) target=x86_64-apple-darwin ;;
                darwin-arm64) target=aarch64-apple-darwin ;;
            esac
            tmp=$(mktemp -d)
            url="https://github.com/astral-sh/uv/releases/latest/download/uv-$target.tar.gz"
            fetch_summed "$url" "$url.sha256" "$tmp/uv.tar.gz"
            extract "$tmp/uv.tar.gz" "$tmp/uv" 1
            rm -rf "$dir" && mv "$tmp/uv" "$dir" && rm -rf "$tmp"
        ' _ "${BASH_SOURCE[0]}" "$dir"
    fi
    echo "$dir"
}

# weft_uv ARGS...: run uv (need_uv) with everything it keeps inside
# WEFT_NEED_DIR: the Pythons it downloads (`python`, never the machine's
# own), the tools it installs (`tools`, their programs in `bin`) and its
# cache (`.uv-cache`, copied from, never linked to, so a recipe may delete
# it once done). The person's own uv config is left out.
weft_uv() {
    local uv
    uv=$(need_uv)
    UV_PYTHON_INSTALL_DIR="$WEFT_NEED_DIR/python" UV_PYTHON_PREFERENCE=only-managed \
        UV_PYTHON_BIN_DIR="$WEFT_NEED_DIR/.uv-python-bin" \
        UV_TOOL_DIR="$WEFT_NEED_DIR/tools" UV_TOOL_BIN_DIR="$WEFT_NEED_DIR/bin" \
        UV_CACHE_DIR="$WEFT_NEED_DIR/.uv-cache" UV_LINK_MODE=copy UV_NO_CONFIG=1 \
        "$uv/uv" "$@"
}

# github_latest OWNER/REPO: print the tag of the repository's latest GitHub
# release, read off the redirect of its `releases/latest` page (which, unlike
# api.github.com, takes nothing from GitHub's hourly quota for anonymous calls).
github_latest() {
    python3 - "$1" <<'PY'
import sys, urllib.parse, urllib.request
repo = sys.argv[1]
req = urllib.request.Request(f"https://github.com/{repo}/releases/latest", headers={"User-Agent": "weft-agent-install"})
with urllib.request.urlopen(req, timeout=60) as r:
    url = r.geturl()
if "/releases/tag/" not in url:
    sys.exit(f"github.com/{repo} has no latest release (its releases/latest page went to {url})")
print(urllib.parse.unquote(url.rsplit("/", 1)[1]))
PY
}
