#!/usr/bin/env bash
# install.sh: build and install the weft CLI, the VS Code
# extension, and (when a daemon is up) the dispatcher + listener
# images.
#
# The dispatcher and listener run as Pod images inside the local
# kind cluster. Their source lives in this workspace, so a CLI
# rebuild means the dispatcher needs a rebuild + restart too;
# otherwise the running pod silently lags behind the CLI. This
# script handles that for you.
#
# Usage:
#   ./install.sh                  # build CLI + extension (+ refresh daemon if running)
#   ./install.sh --cli-only       # CLI only
#   ./install.sh --extension-only # VS Code extension only
#   ./install.sh --no-daemon      # skip daemon refresh (CLI install only)
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
refresh_daemon=1

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
      refresh_daemon=0
      shift
      ;;
    --no-daemon)
      refresh_daemon=0
      shift
      ;;
    -h|--help)
      cat <<EOF
Usage: ./install.sh [--cli-only|--extension-only|--no-daemon] [--debug] [--prefix PATH] [--uninstall]

Default: builds the Rust CLI (\`weft\`, symlinked into PREFIX/bin),
the VS Code extension (\`extension-vscode/weft-vscode-*.vsix\`),
AND, if the daemon is currently running, rebuilds + restarts the
dispatcher and listener images so they pick up CLI/dispatcher
source changes.

If the daemon is NOT running, the rebuild step is skipped; the
next \`weft daemon start\` will pick up the new sources.

  --cli-only        Skip the VS Code extension. (Daemon refresh still runs.)
  --extension-only  Skip the Rust CLI and the daemon refresh.
  --no-daemon       Build the CLI but never touch the daemon.
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
  echo "CLI done."
fi

# ─── Daemon refresh ───────────────────────────────────────────────
#
# A CLI rebuild that touches `weft-compiler` or `weft-dispatcher`
# means the dispatcher pod is now running stale code. Detect a
# live daemon and rebuild + restart the images. Skipped if the
# daemon isn't up (next `weft daemon start` will pick up changes
# anyway).

if [[ $refresh_daemon -eq 1 ]]; then
  weft_bin="${bin_dir}/weft"
  if [[ ! -x "${weft_bin}" && ! -L "${weft_bin}" ]]; then
    echo "  no weft binary at ${weft_bin}; skipping daemon refresh"
  elif ! command -v docker >/dev/null 2>&1; then
    echo "  docker not on PATH; skipping daemon refresh"
  elif ! command -v kubectl >/dev/null 2>&1; then
    echo "  kubectl not on PATH; skipping daemon refresh"
  else
    # Probe the dispatcher's HTTP /health. If it answers, the
    # daemon is up and we should refresh; if not, skip.
    dispatcher_url="${WEFT_DISPATCHER_URL:-http://127.0.0.1:9999}"
    if curl --silent --max-time 2 "${dispatcher_url}/health" >/dev/null 2>&1; then
      echo
      echo "rebuilding dispatcher + listener images and restarting…"
      WEFT_REPO_ROOT="${here}" "${weft_bin}" daemon restart --rebuild
    else
      echo "  daemon not running at ${dispatcher_url}; skipping refresh"
      echo "  (next \`weft daemon start\` will build fresh images)"
    fi
  fi
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
