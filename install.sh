#!/usr/bin/env bash
# install.sh — build and install the weft CLI + the VS Code
# extension.
#
# The dispatcher and listener are no longer host binaries: they run
# as Pod images inside a local kind cluster. `weft daemon start`
# builds those images on first use. The only host-side binary is
# `weft` itself.
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

By default builds BOTH the Rust CLI (\`weft\`, symlinked into
PREFIX/bin) AND the VS Code extension (packaged as
extension-vscode/weft-vscode-*.vsix; install it yourself with
\`code --install-extension <vsix>\`).

The dispatcher + listener images are NOT built or installed here;
\`weft daemon start\` builds them on demand and loads them into a
kind cluster.

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
binaries=(weft)

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
    cargo build --release -p weft-cli
    target_dir="${here}/target/release"
  else
    cargo build -p weft-cli
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
  echo "CLI done. run \`weft daemon start\` to bring up the kind cluster."
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
  # --allow-missing-repository skips the "repository field missing"
  # interactive prompt so the script never blocks on stdin.
  echo
  echo "packaging .vsix…"
  pnpm dlx @vscode/vsce package \
    --no-dependencies \
    --allow-missing-repository \
    --skip-license

  vsix="$(ls -t weft-vscode-*.vsix 2>/dev/null | head -n 1 || true)"
  popd >/dev/null

  if [[ -z "${vsix}" ]]; then
    echo "vsce ran but no .vsix was produced" >&2
    exit 1
  fi

  echo
  echo "VS Code extension done: ${ext_dir}/${vsix}"

  # Auto-install if `code` is on PATH. --force overwrites the
  # existing install without prompting. `code` talks to the running
  # editor through an IPC socket (VSCODE_IPC_HOOK_CLI); under
  # WSL/remote-SSH this env var often points at a dead socket from
  # a closed terminal. Pick the newest LIVE socket under
  # /run/user/$UID/ instead.
  pick_live_vscode_socket() {
    local run_dir="/run/user/${UID}"
    [[ -d "${run_dir}" ]] || return 1
    # Newest socket first; return the first one whose peer accepts
    # a connection. `socat -u` is the portable probe; fall back to
    # `nc -U` with a short timeout.
    local sock
    for sock in $(ls -1t "${run_dir}"/vscode-ipc-*.sock 2>/dev/null); do
      if command -v socat >/dev/null 2>&1; then
        if timeout 0.3 socat -u /dev/null UNIX-CONNECT:"${sock}" >/dev/null 2>&1; then
          printf '%s' "${sock}"
          return 0
        fi
      elif command -v nc >/dev/null 2>&1; then
        if timeout 0.3 nc -U -z "${sock}" >/dev/null 2>&1; then
          printf '%s' "${sock}"
          return 0
        fi
      else
        # No probe available: trust the newest socket. Worst case
        # the install fails and the user sees the copy-paste hint.
        printf '%s' "${sock}"
        return 0
      fi
    done
    return 1
  }

  if command -v code >/dev/null 2>&1; then
    echo "installing into VS Code…"
    if live_sock="$(pick_live_vscode_socket)"; then
      VSCODE_IPC_HOOK_CLI="${live_sock}" code --install-extension "${ext_dir}/${vsix}" --force
    elif ! code --install-extension "${ext_dir}/${vsix}" --force; then
      echo
      echo "  auto-install failed (no live VS Code socket found)."
      echo "  run this inside a VS Code terminal:"
      echo "    code --install-extension '${ext_dir}/${vsix}' --force"
    fi
  else
    echo "  'code' not on PATH. install manually with:"
    echo "    code --install-extension '${ext_dir}/${vsix}' --force"
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
