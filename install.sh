#!/usr/bin/env bash
# install.sh — build the weft CLI + dispatcher + runner in release
# mode and symlink them into ~/.local/bin, and/or build the VS Code
# extension into a .vsix package.
#
# Usage:
#   ./install.sh                  # build + install both (CLI + extension)
#   ./install.sh --cli-only       # build + install the CLI only
#   ./install.sh --extension-only # build the VS Code extension only
#   ./install.sh --debug          # use debug profile for the CLI
#   ./install.sh --prefix /path   # install CLI into /path/bin instead
#   ./install.sh --uninstall      # remove the CLI symlinks

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
profile="release"
prefix="${HOME}/.local"
uninstall=0
build_cli=1
build_ext=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug)
      profile="dev"
      shift
      ;;
    --prefix)
      prefix="$2"
      shift 2
      ;;
    --uninstall)
      uninstall=1
      shift
      ;;
    --cli-only)
      build_ext=0
      shift
      ;;
    --extension-only)
      build_cli=0
      shift
      ;;
    -h|--help)
      cat <<EOF
Usage: ./install.sh [--cli-only|--extension-only] [--debug] [--prefix PATH] [--uninstall]

By default builds BOTH the Rust CLI (weft / weft-dispatcher / weft-
runner, symlinked into PREFIX/bin) AND the VS Code extension
(packaged as extension-vscode/weft-vscode-*.vsix; install it
yourself with \`code --install-extension <vsix>\`).

  --cli-only        Skip the VS Code extension.
  --extension-only  Skip the Rust CLI.
  --debug           Build CLI with the debug profile.
  --prefix PATH     Install CLI into PATH/bin instead of ~/.local/bin.
  --uninstall       Remove installed CLI symlinks (no build).
EOF
      exit 0
      ;;
    *)
      echo "unknown flag: $1" >&2
      exit 1
      ;;
  esac
done

bin_dir="${prefix}/bin"
binaries=(weft weft-dispatcher weft-runner)

# ─── uninstall ────────────────────────────────────────────────────

if [[ $uninstall -eq 1 ]]; then
  for b in "${binaries[@]}"; do
    target="${bin_dir}/${b}"
    if [[ -L "${target}" || -f "${target}" ]]; then
      rm -f "${target}"
      echo "removed ${target}"
    fi
  done
  exit 0
fi

cd "${here}"

# ─── CLI ──────────────────────────────────────────────────────────

if [[ $build_cli -eq 1 ]]; then
  mkdir -p "${bin_dir}"

  if [[ "${profile}" == "release" ]]; then
    cargo build --release -p weft-cli -p weft-dispatcher -p weft-runner
    target_dir="${here}/target/release"
  else
    cargo build -p weft-cli -p weft-dispatcher -p weft-runner
    target_dir="${here}/target/debug"
  fi

  for b in "${binaries[@]}"; do
    src="${target_dir}/${b}"
    dst="${bin_dir}/${b}"
    if [[ ! -x "${src}" ]]; then
      echo "build output missing: ${src}" >&2
      exit 1
    fi
    ln -sfn "${src}" "${dst}"
    echo "linked ${dst} -> ${src}"
  done
  echo
  echo "CLI done. binaries live under ${bin_dir}."
fi

# ─── VS Code extension ────────────────────────────────────────────

if [[ $build_ext -eq 1 ]]; then
  ext_dir="${here}/extension-vscode"
  if [[ ! -d "${ext_dir}" ]]; then
    echo "extension-vscode/ not found, skipping extension build" >&2
    exit 1
  fi

  # Node version gate. Vite 7 + @sveltejs/vite-plugin-svelte 6 need
  # Node 20.19+. If the user has nvm, suggest it; otherwise just bail
  # with a clear message.
  node_major="$(node -v 2>/dev/null | sed -E 's/^v([0-9]+).*/\1/' || echo 0)"
  if [[ "${node_major}" -lt 20 ]]; then
    echo "extension build needs Node 20.19+ (got $(node -v 2>/dev/null || echo 'none'))" >&2
    echo "  try: nvm use 24 && ./install.sh --extension-only" >&2
    exit 1
  fi

  if ! command -v pnpm >/dev/null 2>&1; then
    echo "pnpm not on PATH. install via: npm i -g pnpm" >&2
    exit 1
  fi

  pushd "${ext_dir}" >/dev/null

  echo
  echo "building VS Code extension…"
  pnpm install --prefer-offline
  pnpm run compile
  pnpm run bundle:webview

  # vsce packages the .vsix. We call it through pnpm dlx so it doesn't
  # have to be globally installed. --no-dependencies tells vsce to
  # skip the npm-list check (pnpm's symlinked layout confuses it; the
  # deps that matter get bundled by Vite anyway).
  echo
  echo "packaging .vsix…"
  pnpm dlx @vscode/vsce package --no-dependencies

  vsix="$(ls -t weft-vscode-*.vsix 2>/dev/null | head -n 1 || true)"
  popd >/dev/null

  if [[ -n "${vsix}" ]]; then
    echo
    echo "VS Code extension done: ${ext_dir}/${vsix}"
    echo "  install it with: code --install-extension '${ext_dir}/${vsix}'"
  else
    echo "vsce ran but no .vsix was produced" >&2
    exit 1
  fi
fi

# ─── PATH hint ────────────────────────────────────────────────────

if [[ $build_cli -eq 1 ]]; then
  case ":${PATH}:" in
    *":${bin_dir}:"*)
      echo
      echo "${bin_dir} is on PATH. try: weft --help"
      ;;
    *)
      echo
      echo "  ${bin_dir} is NOT on your PATH."
      echo "  add this to your shell rc (bash/zsh):"
      echo
      echo "    export PATH=\"${bin_dir}:\$PATH\""
      echo
      ;;
  esac
fi
