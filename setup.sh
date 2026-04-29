#!/usr/bin/env bash
# setup.sh: build and install everything weft-related on the local
# machine.
#
# By default, runs the install pipeline for the everyday loop:
#   - CLI                  (cargo build, symlink into ~/.local/bin)
#   - Daemon               (rebuild dispatcher/listener images and
#                          restart the kind pod if it's running;
#                          if it's not, start it fresh)
#   - VS Code extension    (compile, package .vsix, install into VS Code)
#
# The browser extension is OPT-IN via `--browser` since rebuilding
# it bumps versions and signs Firefox, which is heavier than most
# rebuild loops need.
#
# Component flags pick a subset (multiple combine):
#   --cli         build CLI only
#   --daemon      refresh daemon only
#   --vscode      build/install VS Code extension only
#   --browser     build browser extension only (bumps, signs, zips)
#   (e.g. --cli --daemon does both, skips the VS Code extension)
#
# Default-on knobs you opt OUT of:
#   --no-bump     skip the browser-extension version bump
#   --no-sign     skip the Firefox AMO signing step
#   --no-daemon   skip the daemon refresh (when CLI is being built)
#
# Browser-target flags (default: every browser):
#   --chrome      Chrome / Brave / Vivaldi / Arc / Edge / Opera unpacked
#   --firefox
#   --edge
#   --opera
#   --safari
#   (multiple combine)
#
# CLI knobs:
#   --debug       build CLI with the debug profile
#   --prefix PATH install CLI binary into PATH/bin (default ~/.local)
#
# Removal:
#   --uninstall   Remove user-facing pieces but preserve work. Stops
#                 the daemon (graceful: leases released on the Pod's
#                 SIGTERM), uninstalls the VS Code extension, drops
#                 the CLI symlink. PRESERVES: kind cluster,
#                 postgres data, docker images, BuildKit cache,
#                 cargo target/, image-hash stamps, browser
#                 extensions. Reinstall via ./setup.sh and your
#                 projects + history come back instantly.
#   --purge       TRUE clean slate. Deletes the kind cluster, every
#                 weft-built docker image (dispatcher, listener,
#                 every weft-worker-*), every weavemind sidecar
#                 image, the BuildKit cache, the workspace target/
#                 cargo cache, ~/.local/share/weft (image-hash
#                 stamps, port-forward state, etc), and every
#                 extension build artifact. The next install pays
#                 a full cold-rebuild cost. Can combine with
#                 --uninstall.
#
#                 SHARED base images (commonly reused by other
#                 docker projects on the host) are kept by default.
#                 Add the matching flag to remove them too:
#                   --postgres   remove postgres:16-alpine
#                   --kind       remove kindest/node images
#                   --debian     remove debian:bookworm-slim
#
# Examples:
#   ./setup.sh                          # full install
#   ./setup.sh --browser                # rebuild browser ext only
#   ./setup.sh --browser --no-sign      # browser ext, skip AMO signing
#   ./setup.sh --browser --chrome       # browser ext, Chrome only
#   ./setup.sh --cli --daemon           # CLI + daemon, no extensions
#   ./setup.sh --uninstall              # remove installed pieces
#   ./setup.sh --uninstall --purge      # remove + nuke local state
#   ./setup.sh --uninstall --purge --postgres --kind --debian
#                                       # purge including shared base images

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- visual library --------------------------------------------------
#
# Colors + symbols. Disabled when stdout isn't a TTY (CI pipelines,
# `setup.sh > log.txt`) or when NO_COLOR is set, so the output stays
# clean and grep-able.

if [[ -t 1 && "${NO_COLOR:-}" == "" ]]; then
  C_RESET=$'\033[0m'
  C_DIM=$'\033[2m'
  C_BOLD=$'\033[1m'
  C_RED=$'\033[31m'
  C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'
  C_BLUE=$'\033[34m'
  C_MAGENTA=$'\033[35m'
  C_CYAN=$'\033[36m'
  HAS_TTY=1
else
  C_RESET=""
  C_DIM=""
  C_BOLD=""
  C_RED=""
  C_GREEN=""
  C_YELLOW=""
  C_BLUE=""
  C_MAGENTA=""
  C_CYAN=""
  HAS_TTY=0
fi

# Symbols: braille spinner frames + status glyphs. Plain ASCII
# fallbacks aren't worth the branching; every modern terminal
# (kitty, alacritty, iTerm2, Windows Terminal, vscode terminal,
# gnome-terminal) renders these correctly.
SP_FRAMES=(⠋ ⠙ ⠹ ⠸ ⠼ ⠴ ⠦ ⠧ ⠇ ⠏)
SYM_OK="✓"
SYM_FAIL="✗"
SYM_STEP="▸"
SYM_INFO="ℹ"
SYM_WARN="⚠"
SYM_ARROW="→"

section() {
  printf '\n%s%s%s %s%s%s\n' "${C_BOLD}" "${C_BLUE}" "▶" "${C_BOLD}" "$*" "${C_RESET}"
}
ok()   { printf '  %s%s%s %s\n' "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "$*"; }
fail() { printf '  %s%s%s %s\n' "${C_RED}" "${SYM_FAIL}" "${C_RESET}" "$*" >&2; }
step() { printf '  %s%s%s %s\n' "${C_CYAN}" "${SYM_STEP}" "${C_RESET}" "$*"; }
hint() { printf '  %s%s %s%s\n' "${C_DIM}" "${SYM_INFO}" "$*" "${C_RESET}"; }
warn() { printf '  %s%s%s %s\n' "${C_YELLOW}" "${SYM_WARN}" "${C_RESET}" "$*"; }

# spin "label" cmd args... -- run cmd while showing a spinner. On
# success replace the spinner with `✓ label`. On failure, replace
# with `✗ label` and forward the captured output to stderr so the
# user sees what went wrong. Plain echo fallback when no TTY.
spin() {
  local label="$1"; shift
  if [[ $HAS_TTY -eq 0 ]]; then
    printf '  %s %s\n' "${SYM_STEP}" "${label}"
    if "$@" >/tmp/weft-setup-spin.log 2>&1; then
      ok "${label}"
      rm -f /tmp/weft-setup-spin.log
      return 0
    else
      local rc=$?
      fail "${label}"
      cat /tmp/weft-setup-spin.log >&2 || true
      rm -f /tmp/weft-setup-spin.log
      return $rc
    fi
  fi
  local logfile
  logfile="$(mktemp -t weft-setup.XXXXXX.log)"
  "$@" >"${logfile}" 2>&1 &
  local pid=$!
  local i=0
  printf '  '
  while kill -0 "${pid}" 2>/dev/null; do
    local frame="${SP_FRAMES[$((i % ${#SP_FRAMES[@]}))]}"
    printf '\r  %s%s%s %s' "${C_CYAN}" "${frame}" "${C_RESET}" "${label}"
    sleep 0.08
    i=$((i + 1))
  done
  if wait "${pid}"; then
    printf '\r  %s%s%s %s\n' "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${label}"
    rm -f "${logfile}"
    return 0
  else
    local rc=$?
    printf '\r  %s%s%s %s\n' "${C_RED}" "${SYM_FAIL}" "${C_RESET}" "${label}"
    cat "${logfile}" >&2 || true
    rm -f "${logfile}"
    return $rc
  fi
}

# spin_passthrough "label" cmd args... -- like spin but lets the
# command print its OWN output live. Used for cargo/docker where
# users want to watch progress. Just runs the command directly,
# bracketed by a step header and a final ✓ / ✗.
spin_passthrough() {
  local label="$1"; shift
  step "${label}"
  if "$@"; then
    ok "${label}"
    return 0
  else
    local rc=$?
    fail "${label}"
    return $rc
  fi
}

# ---- defaults --------------------------------------------------------

profile="release"
prefix="${HOME}/.local"
do_uninstall=0
do_purge=0
purge_debian=0
purge_kind=0
purge_postgres=0

# Components: 0 = excluded, 1 = included. The default install set
# is CLI + daemon + VS Code extension. The browser extension is
# OPT-IN via --browser since it's heavier (extension store sign,
# version bump, full per-target build) and most rebuild loops
# don't need it.
#
# When the user passes any --<component> we flip every component
# to 0 first so the listed flags act as opt-ins.
build_cli=1
refresh_daemon=1
build_vscode=1
build_browser=0
component_flag_seen=0

# Browser-target subset: same logic as components.
target_chrome=1
target_firefox=1
target_edge=1
target_opera=1
target_safari=1
target_flag_seen=0

# Negation flags
do_bump=1
do_sign=1

# Called the first time a --<component> flag is seen. Zeroes every
# component so subsequent flags act as opt-ins. No-op after the
# first call.
components_flip() {
  if [[ $component_flag_seen -eq 0 ]]; then
    build_cli=0
    refresh_daemon=0
    build_vscode=0
    build_browser=0
    component_flag_seen=1
  fi
}
targets_flip() {
  if [[ $target_flag_seen -eq 0 ]]; then
    target_chrome=0
    target_firefox=0
    target_edge=0
    target_opera=0
    target_safari=0
    target_flag_seen=1
  fi
}

# ---- argv ------------------------------------------------------------

while [[ $# -gt 0 ]]; do
  case "$1" in
    --uninstall) do_uninstall=1 ;;
    --purge)     do_purge=1 ;;
    --debian)    purge_debian=1 ;;
    --kind)      purge_kind=1 ;;
    --postgres)  purge_postgres=1 ;;

    --cli)       components_flip; build_cli=1 ;;
    --daemon)    components_flip; refresh_daemon=1 ;;
    --vscode)    components_flip; build_vscode=1 ;;
    --browser)   components_flip; build_browser=1 ;;

    --no-bump)   do_bump=0 ;;
    --no-sign)   do_sign=0 ;;
    --no-daemon) refresh_daemon=0 ;;

    --chrome)    targets_flip; target_chrome=1 ;;
    --firefox)   targets_flip; target_firefox=1 ;;
    --edge)      targets_flip; target_edge=1 ;;
    --opera)     targets_flip; target_opera=1 ;;
    --safari)    targets_flip; target_safari=1 ;;

    --debug)     profile="dev" ;;
    --prefix)    shift; prefix="$1" ;;

    -h|--help)
      sed -n '2,75p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      fail "unknown flag: $1 (run --help for the list)"
      exit 1
      ;;
  esac
  shift
done

bin_dir="${prefix}/bin"
weft_bin="${bin_dir}/weft"

# ---- pre-flight: required binaries ----------------------------------
#
# Check up front so we fail fast instead of midway through. Each
# component lists what it needs. Missing pieces get listed all at
# once (not one at a time) so the user installs everything in one
# round trip.

if [[ $do_uninstall -eq 0 && $do_purge -eq 0 ]]; then
  missing=()
  install_hints=()

  if [[ $build_cli -eq 1 ]]; then
    if ! command -v cargo >/dev/null 2>&1; then
      missing+=("cargo (Rust toolchain)")
      install_hints+=("  Rust:    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh")
    fi
  fi

  if [[ $refresh_daemon -eq 1 ]]; then
    if ! command -v docker >/dev/null 2>&1; then
      missing+=("docker")
      install_hints+=("  Docker:  https://docs.docker.com/get-docker/")
    fi
    if ! command -v kubectl >/dev/null 2>&1; then
      missing+=("kubectl")
      install_hints+=("  kubectl: https://kubernetes.io/docs/tasks/tools/")
    fi
    if ! command -v kind >/dev/null 2>&1; then
      missing+=("kind")
      install_hints+=("  kind:    https://kind.sigs.k8s.io/docs/user/quick-start/#installation")
    fi
  fi

  if [[ $build_vscode -eq 1 ]]; then
    node_major="$(node -v 2>/dev/null | sed -E 's/^v([0-9]+).*/\1/' || echo 0)"
    if [[ "${node_major}" -lt 20 ]]; then
      missing+=("node 20+")
      install_hints+=("  Node:    https://nodejs.org/en/download (or 'nvm install 20')")
    fi
    if ! command -v pnpm >/dev/null 2>&1; then
      missing+=("pnpm")
      install_hints+=("  pnpm:    npm i -g pnpm")
    fi
    if ! command -v code >/dev/null 2>&1; then
      # Not fatal: we can still build the .vsix; the user just
      # needs to install it manually. Surface as a soft warning
      # later in the VS Code section, not a pre-flight failure.
      :
    fi
  fi

  if [[ $build_browser -eq 1 ]]; then
    if ! command -v node >/dev/null 2>&1; then
      missing+=("node 20+ (browser extension)")
      install_hints+=("  Node:    https://nodejs.org/en/download")
    fi
    if ! command -v pnpm >/dev/null 2>&1; then
      missing+=("pnpm (browser extension)")
      install_hints+=("  pnpm:    npm i -g pnpm")
    fi
  fi

  if [[ ${#missing[@]} -gt 0 ]]; then
    # Dedupe (node + pnpm may be requested by multiple components).
    seen=()
    unique_missing=()
    for item in "${missing[@]}"; do
      already=0
      for s in "${seen[@]:-}"; do [[ "$s" == "$item" ]] && already=1; done
      if [[ $already -eq 0 ]]; then
        unique_missing+=("$item")
        seen+=("$item")
      fi
    done
    seen_hints=()
    unique_hints=()
    for item in "${install_hints[@]}"; do
      already=0
      for s in "${seen_hints[@]:-}"; do [[ "$s" == "$item" ]] && already=1; done
      if [[ $already -eq 0 ]]; then
        unique_hints+=("$item")
        seen_hints+=("$item")
      fi
    done

    section "Pre-flight"
    fail "missing required tools:"
    for m in "${unique_missing[@]}"; do
      printf '    %s%s%s %s\n' "${C_RED}" "${SYM_ARROW}" "${C_RESET}" "${m}" >&2
    done
    printf '\n'
    hint "install:"
    for h in "${unique_hints[@]}"; do
      printf '    %s%s%s\n' "${C_DIM}" "${h#  }" "${C_RESET}" >&2
    done
    printf '\n'
    hint "re-run ${C_BOLD}./setup.sh${C_RESET}${C_DIM} once everything above is on PATH${C_RESET}"
    exit 1
  fi
fi

# ---- uninstall / purge ----------------------------------------------

if [[ $do_uninstall -eq 1 || $do_purge -eq 1 ]]; then
  if [[ $do_uninstall -eq 1 ]]; then
    section "Uninstall"
    hint "${C_DIM}preserving cluster + data; pass --purge to wipe everything${C_RESET}"

    # 1. Stop the daemon. Scales the StatefulSet to 0 which fires
    #    SIGTERM on the Pod, which triggers the dispatcher's
    #    graceful_shutdown to release leases cleanly. Best-effort:
    #    a missing CLI or already-stopped daemon doesn't fail.
    if command -v "${weft_bin}" >/dev/null 2>&1 || [[ -L "${weft_bin}" ]]; then
      "${weft_bin}" daemon stop >/dev/null 2>&1 || true
      ok "daemon stopped (cluster + data kept)"
    else
      hint "daemon: no weft binary on disk; nothing to stop"
    fi

    # 2. Remove the VS Code extension if `code` is on PATH. Uses
    #    the same live-IPC-socket discovery as the install side
    #    so an open VS Code window picks up the change without a
    #    manual reload.
    if command -v code >/dev/null 2>&1; then
      pick_live_vscode_socket() {
        local run_dir="/run/user/${UID}"
        [[ -d "${run_dir}" ]] || return 1
        local sock
        for sock in $(ls -1t "${run_dir}"/vscode-ipc-*.sock 2>/dev/null); do
          if command -v socat >/dev/null 2>&1; then
            if timeout 0.3 socat -u /dev/null UNIX-CONNECT:"${sock}" >/dev/null 2>&1; then
              printf '%s' "${sock}"; return 0
            fi
          elif command -v nc >/dev/null 2>&1; then
            if timeout 0.3 nc -U -z "${sock}" >/dev/null 2>&1; then
              printf '%s' "${sock}"; return 0
            fi
          else
            printf '%s' "${sock}"; return 0
          fi
        done
        return 1
      }
      if live_sock="$(pick_live_vscode_socket)"; then
        VSCODE_IPC_HOOK_CLI="${live_sock}" \
          code --uninstall-extension weavemindai.weft-vscode >/dev/null 2>&1 || true
      else
        code --uninstall-extension weavemindai.weft-vscode >/dev/null 2>&1 || true
      fi
      ok "VS Code extension uninstalled (if present)"
    else
      hint "VS Code: 'code' not on PATH; skipping extension removal"
    fi

    # 3. Drop the CLI symlink.
    if [[ -L "${weft_bin}" || -f "${weft_bin}" ]]; then
      rm -f "${weft_bin}"
      ok "removed ${C_DIM}${weft_bin}${C_RESET}"
    else
      hint "CLI symlink at ${weft_bin} already absent"
    fi

    # 4. Hints for the manual cleanup we deliberately don't do.
    #    Only relevant when uninstall runs without --purge; if the
    #    user chained --purge, those things are already gone.
    if [[ $do_purge -eq 0 ]]; then
      printf '\n%s%sWhat is preserved:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
      printf '  %skind cluster%s ${C_DIM}\x27%s\x27 (postgres, history, projects)%s\n' \
        "${C_CYAN}" "${C_RESET}${C_DIM}" "${WEFT_CLUSTER_NAME:-weft-local}" "${C_RESET}"
      printf '  %sdocker images%s ${C_DIM}(dispatcher, listener, weft-worker-*)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_RESET}"
      printf '  %sworkspace target/%s ${C_DIM}(cargo cache)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_RESET}"
      printf '  %s~/.local/share/weft/%s ${C_DIM}(port-forward state, build hashes)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_RESET}"
      printf '  %sbrowser extensions%s ${C_DIM}(remove manually from each browser if installed)%s\n' \
        "${C_CYAN}" "${C_RESET}" "${C_RESET}"
      printf '\n%sTo wipe everything too:%s ./setup.sh --uninstall --purge\n' \
        "${C_DIM}" "${C_RESET}"
    fi
  fi

  if [[ $do_purge -eq 1 ]]; then
    section "Purge"
    hint "${C_DIM}true clean slate; next install pays a full rebuild cost${C_RESET}"

    # 1. Delete the kind cluster.
    if command -v kind >/dev/null 2>&1; then
      cluster="${WEFT_CLUSTER_NAME:-weft-local}"
      if kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
        kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
        ok "kind cluster ${C_DIM}'${cluster}'${C_RESET} deleted"
      else
        hint "kind cluster ${C_DIM}'${cluster}'${C_RESET}: not present"
      fi
    fi

    # 2. Reclaim every weft-related host docker image.
    if command -v docker >/dev/null 2>&1; then
      for tag in weft-dispatcher:local weft-listener:local; do
        docker image rm -f "${tag}" >/dev/null 2>&1 || true
      done
      ok "removed dispatcher + listener images"

      docker image prune --force \
        --filter "label=weft.dev/project" >/dev/null 2>&1 || true
      stale_worker_ids="$(docker images 'weft-worker-*' -q 2>/dev/null | sort -u)"
      if [[ -n "${stale_worker_ids}" ]]; then
        echo "${stale_worker_ids}" | xargs docker rmi -f >/dev/null 2>&1 || true
      fi
      ok "removed every weft-worker-* image"

      sidecar_ids="$(
        docker images 'ghcr.io/weavemindai/sidecar-*' -q 2>/dev/null | sort -u
      )"
      if [[ -n "${sidecar_ids}" ]]; then
        echo "${sidecar_ids}" | xargs docker rmi -f >/dev/null 2>&1 || true
        ok "removed sidecar images"
      fi

      # Shared base images, gated.
      if [[ $purge_postgres -eq 1 ]]; then
        docker image rm -f postgres:16-alpine >/dev/null 2>&1 || true
        ok "removed postgres:16-alpine ${C_DIM}(--postgres)${C_RESET}"
      fi
      if [[ $purge_kind -eq 1 ]]; then
        kind_ids="$(docker images kindest/node -q 2>/dev/null | sort -u)"
        if [[ -n "${kind_ids}" ]]; then
          echo "${kind_ids}" | xargs docker rmi -f >/dev/null 2>&1 || true
          ok "removed kindest/node images ${C_DIM}(--kind)${C_RESET}"
        fi
      fi
      if [[ $purge_debian -eq 1 ]]; then
        docker image rm -f debian:bookworm-slim >/dev/null 2>&1 || true
        ok "removed debian:bookworm-slim ${C_DIM}(--debian)${C_RESET}"
      fi

      docker buildx prune --force >/dev/null 2>&1 || true
      ok "pruned BuildKit cache"
    fi

    # 3. Workspace cargo target/.
    if [[ -d "${here}/target" ]]; then
      rm -rf "${here}/target"
      ok "removed ${C_DIM}target/${C_RESET}"
    fi

    # 4. Daemon-local state.
    if [[ -d "${HOME}/.local/share/weft" ]]; then
      rm -rf "${HOME}/.local/share/weft"
      ok "removed ${C_DIM}~/.local/share/weft/${C_RESET}"
    fi

    # 5. Browser-extension build artifacts.
    removed_browser_any=0
    for path in \
      "${here}/extension-build" \
      "${here}/extension-browser/.output" \
      "${here}/extension-browser/web-ext-artifacts"; do
      if [[ -d "${path}" ]]; then
        rm -rf "${path}"
        removed_browser_any=1
      fi
    done
    if [[ ${removed_browser_any} -eq 1 ]]; then
      ok "removed browser-extension build artifacts"
    fi

    # 6. VS Code extension's pre-packaged .vsix.
    if compgen -G "${here}/extension-vscode/weft-vscode-*.vsix" >/dev/null; then
      rm -f "${here}/extension-vscode/"weft-vscode-*.vsix
      ok "removed packaged ${C_DIM}weft-vscode-*.vsix${C_RESET}"
    fi

    # Shared-base hint footer.
    skipped_lines=()
    if [[ $purge_postgres -eq 0 ]]; then
      if docker image inspect postgres:16-alpine >/dev/null 2>&1; then
        skipped_lines+=("postgres:16-alpine|--postgres")
      fi
    fi
    if [[ $purge_kind -eq 0 ]]; then
      if [[ -n "$(docker images kindest/node -q 2>/dev/null)" ]]; then
        skipped_lines+=("kindest/node|--kind")
      fi
    fi
    if [[ $purge_debian -eq 0 ]]; then
      if docker image inspect debian:bookworm-slim >/dev/null 2>&1; then
        skipped_lines+=("debian:bookworm-slim|--debian")
      fi
    fi
    if [[ ${#skipped_lines[@]} -gt 0 ]]; then
      printf '\n  %s%sShared base images kept%s ${C_DIM}(reused by other docker projects)${C_RESET}\n' \
        "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
      for entry in "${skipped_lines[@]}"; do
        skipped_image="${entry%%|*}"
        skipped_flag="${entry##*|}"
        printf '    %s%s%s %-22s %s%s%s\n' \
          "${C_DIM}" "${SYM_ARROW}" "${C_RESET}" "${skipped_image}" \
          "${C_DIM}" "(${skipped_flag})" "${C_RESET}"
      done
      printf '\n  %sopt in:%s ./setup.sh --uninstall --purge --postgres --kind --debian\n' \
        "${C_DIM}" "${C_RESET}"
    fi
  fi

  printf '\n%s%s%s ${C_BOLD}Done.${C_RESET}\n' "${C_GREEN}" "${SYM_OK}" "${C_RESET}"
  exit 0
fi

# ---- CLI -------------------------------------------------------------

if [[ $build_cli -eq 1 ]]; then
  section "CLI"
  hint "${C_DIM}cargo's incremental cache makes re-runs near-instant${C_RESET}"
  mkdir -p "${bin_dir}"
  if [[ "${profile}" == "release" ]]; then
    target_dir="${here}/target/release"
  else
    target_dir="${here}/target/debug"
  fi
  src="${target_dir}/weft"
  # Snapshot the binary's identity before the build. If cargo
  # decides nothing changed, the file's mtime stays put and we
  # skip the worker-image prune. Only an actual rebuild (engine
  # / core / cli source change) bumps mtime, which is the signal
  # cached worker images may now be stale.
  pre_mtime="$(stat -c %Y "${src}" 2>/dev/null || echo 0)"
  if [[ "${profile}" == "release" ]]; then
    spin_passthrough "cargo build --release -p weft-cli" \
      cargo build --release -p weft-cli
  else
    spin_passthrough "cargo build -p weft-cli" cargo build -p weft-cli
  fi
  if [[ ! -x "${src}" ]]; then
    fail "build output missing: ${src}"
    exit 1
  fi
  ln -sfn "${src}" "${weft_bin}"
  ok "linked ${C_DIM}${weft_bin}${C_RESET} ${SYM_ARROW} ${C_DIM}${src}${C_RESET}"

  post_mtime="$(stat -c %Y "${src}" 2>/dev/null || echo 0)"
  if [[ "${pre_mtime}" != "${post_mtime}" ]]; then
    # CLI binary actually changed → engine/core source likely
    # changed too. Worker images bake those crates in at
    # `weft build` time, so cached images (host docker + kind
    # containerd) are now stale. BuildKit cache is preserved;
    # only the final tagged images go, so the next `weft build`
    # is fast but produces a fresh worker.
    stale_worker_ids="$(docker images 'weft-worker-*' -q 2>/dev/null | sort -u)"
    if [[ -n "${stale_worker_ids}" ]]; then
      echo "${stale_worker_ids}" | xargs docker rmi -f >/dev/null 2>&1 || true
      ok "removed cached weft-worker-* images on host docker"
    fi
    if command -v kind >/dev/null 2>&1; then
      kind_node=""
      for cluster in $(kind get clusters 2>/dev/null); do
        node="${cluster}-control-plane"
        if docker inspect "${node}" >/dev/null 2>&1; then
          kind_node="${node}"
          break
        fi
      done
      if [[ -n "${kind_node}" ]]; then
        kind_worker_tags="$(
          docker exec "${kind_node}" crictl images 2>/dev/null \
            | awk 'NR>1 && $1 ~ /weft-worker-/ {print $1":"$2}' \
            | sort -u
        )"
        if [[ -n "${kind_worker_tags}" ]]; then
          # shellcheck disable=SC2086
          docker exec "${kind_node}" crictl rmi ${kind_worker_tags} >/dev/null 2>&1 || true
          ok "removed cached weft-worker-* images in kind containerd"
        fi
      fi
    fi
  fi
fi

# ---- daemon refresh / start ------------------------------------------
#
# Two cases share this block:
#   1. Daemon already up: rebuild dispatcher/listener images and roll
#      the pod so it picks up the new code.
#   2. Daemon not up: start it. The CLI's `daemon start` builds the
#      fresh images itself, so a separate --rebuild isn't needed.
#
# Either way the user ends up with a running daemon on the latest
# source. Pre-setup.sh behavior was to skip when the daemon was down,
# which forced a manual `weft daemon start` afterwards.

if [[ $refresh_daemon -eq 1 ]]; then
  section "Daemon"
  if [[ ! -x "${weft_bin}" && ! -L "${weft_bin}" ]]; then
    warn "no weft binary at ${weft_bin}; skipping"
  elif ! command -v docker >/dev/null 2>&1; then
    warn "docker not on PATH; skipping"
  elif ! command -v kubectl >/dev/null 2>&1; then
    warn "kubectl not on PATH; skipping"
  else
    dispatcher_url="${WEFT_DISPATCHER_URL:-http://127.0.0.1:9999}"
    if curl --silent --max-time 2 "${dispatcher_url}/health" >/dev/null 2>&1; then
      hint "daemon running at ${C_DIM}${dispatcher_url}${C_RESET}; refreshing (no-op if nothing changed)"
      spin_passthrough "weft daemon restart --rebuild" \
        env WEFT_REPO_ROOT="${here}" "${weft_bin}" daemon restart --rebuild
    else
      hint "daemon not running; first install pulls images and creates the kind cluster (~2-3 min)"
      spin_passthrough "weft daemon start --rebuild" \
        env WEFT_REPO_ROOT="${here}" "${weft_bin}" daemon start --rebuild
    fi
  fi
fi

# ---- VS Code extension -----------------------------------------------

if [[ $build_vscode -eq 1 ]]; then
  section "VS Code extension"
  ext_dir="${here}/extension-vscode"
  if [[ ! -d "${ext_dir}" ]]; then
    fail "${ext_dir} not found"
    exit 1
  fi

  node_major="$(node -v 2>/dev/null | sed -E 's/^v([0-9]+).*/\1/' || echo 0)"
  if [[ "${node_major}" -lt 20 ]]; then
    fail "needs Node 20+ (got $(node -v 2>/dev/null || echo 'none'))"
    hint "try: ${C_BOLD}nvm use 24 && ./setup.sh --vscode${C_RESET}"
    exit 1
  fi
  if ! command -v pnpm >/dev/null 2>&1; then
    fail "pnpm not on PATH"
    hint "install: ${C_BOLD}npm i -g pnpm${C_RESET}"
    exit 1
  fi

  pushd "${ext_dir}" >/dev/null

  # Skip rebuild if nothing under src/ + config + package.json has
  # changed since the last successful build. We hash inputs and
  # compare to a stamp file. The .vsix from the last run is reused
  # for the install step.
  hash_dir="${HOME}/.local/share/weft/vscode-hashes"
  hash_file="${hash_dir}/extension.hash"
  current_hash="$(
    {
      find src -type f \( -name '*.ts' -o -name '*.svelte' -o -name '*.css' \) -print0 \
        2>/dev/null | sort -z | xargs -0 sha256sum 2>/dev/null
      sha256sum package.json pnpm-lock.yaml svelte.config.mjs vite.webview.config.mjs \
        tsconfig.json tsconfig.webview.json language-configuration.json 2>/dev/null
    } | sha256sum | awk '{print $1}'
  )"
  prior_hash="$(cat "${hash_file}" 2>/dev/null || echo)"
  existing_vsix="$(ls -t weft-vscode-*.vsix 2>/dev/null | head -n 1 || true)"

  if [[ -n "${existing_vsix}" && "${current_hash}" == "${prior_hash}" ]]; then
    ok "no source changes; reusing ${C_DIM}${existing_vsix}${C_RESET}"
    vsix="${existing_vsix}"
  else
    spin "pnpm install" pnpm install --prefer-offline
    spin "tsc compile" pnpm run compile
    spin "vite bundle webview" pnpm run bundle:webview
    spin "package .vsix" pnpm dlx @vscode/vsce package \
      --no-dependencies \
      --allow-missing-repository \
      --skip-license
    vsix="$(ls -t weft-vscode-*.vsix 2>/dev/null | head -n 1 || true)"
    if [[ -n "${vsix}" ]]; then
      mkdir -p "${hash_dir}"
      printf '%s' "${current_hash}" >"${hash_file}"
    fi
  fi

  popd >/dev/null

  if [[ -z "${vsix}" ]]; then
    fail "vsce ran but no .vsix was produced"
    exit 1
  fi

  ok "packaged ${C_DIM}${ext_dir}/${vsix}${C_RESET}"

  # Auto-install via `code` IPC. Pick a live socket under
  # /run/user/$UID/ since closed terminals leave dead sockets in
  # VSCODE_IPC_HOOK_CLI under WSL/remote-SSH.
  pick_live_vscode_socket() {
    local run_dir="/run/user/${UID}"
    [[ -d "${run_dir}" ]] || return 1
    local sock
    for sock in $(ls -1t "${run_dir}"/vscode-ipc-*.sock 2>/dev/null); do
      if command -v socat >/dev/null 2>&1; then
        if timeout 0.3 socat -u /dev/null UNIX-CONNECT:"${sock}" >/dev/null 2>&1; then
          printf '%s' "${sock}"; return 0
        fi
      elif command -v nc >/dev/null 2>&1; then
        if timeout 0.3 nc -U -z "${sock}" >/dev/null 2>&1; then
          printf '%s' "${sock}"; return 0
        fi
      else
        printf '%s' "${sock}"; return 0
      fi
    done
    return 1
  }

  if command -v code >/dev/null 2>&1; then
    if live_sock="$(pick_live_vscode_socket)"; then
      if VSCODE_IPC_HOOK_CLI="${live_sock}" \
          code --install-extension "${ext_dir}/${vsix}" --force >/dev/null 2>&1; then
        ok "installed into VS Code (live window)"
      else
        warn "code --install-extension failed via live socket"
        hint "run inside a VS Code terminal: ${C_BOLD}code --install-extension '${ext_dir}/${vsix}' --force${C_RESET}"
      fi
    elif code --install-extension "${ext_dir}/${vsix}" --force >/dev/null 2>&1; then
      ok "installed into VS Code"
    else
      warn "no live VS Code window found"
      hint "run inside a VS Code terminal: ${C_BOLD}code --install-extension '${ext_dir}/${vsix}' --force${C_RESET}"
    fi
  else
    warn "'code' not on PATH"
    hint "install manually: ${C_BOLD}code --install-extension '${ext_dir}/${vsix}' --force${C_RESET}"
  fi
fi

# ---- browser extension -----------------------------------------------

if [[ $build_browser -eq 1 ]]; then
  section "Browser extension"
  hint "${C_DIM}default install does NOT rebuild this; pass --browser to refresh${C_RESET}"
  bx_dir="${here}/extension-browser"
  out_dir="${here}/extension-build"
  if [[ ! -d "${bx_dir}" ]]; then
    fail "${bx_dir} not found"
    exit 1
  fi

  # Sign requires Mozilla AMO API keys. Check up front so we don't
  # waste five minutes building before failing.
  if [[ $do_sign -eq 1 ]]; then
    if [[ -f "${here}/.env.extension" ]]; then
      set -a
      # shellcheck disable=SC1091
      source "${here}/.env.extension"
      set +a
    fi
    if [[ -z "${WEB_EXT_API_KEY:-}" || -z "${WEB_EXT_API_SECRET:-}" ]]; then
      fail "--sign needs WEB_EXT_API_KEY + WEB_EXT_API_SECRET in ${here}/.env.extension"
      hint "get keys: ${C_DIM}https://addons.mozilla.org/en-US/developers/addon/api/key/${C_RESET}"
      hint "or pass ${C_BOLD}--no-sign${C_RESET} to skip Firefox signing"
      exit 1
    fi
    if ! command -v web-ext >/dev/null 2>&1; then
      fail "--sign needs web-ext on PATH"
      hint "install: ${C_BOLD}pnpm i -g web-ext${C_RESET} (or pass --no-sign)"
      exit 1
    fi
  fi

  pushd "${bx_dir}" >/dev/null

  # Bump version unless --no-bump.
  current_version="$(node -p "require('./package.json').version")"
  if [[ $do_bump -eq 1 ]]; then
    npm version patch --no-git-tag-version >/dev/null
  fi
  version="$(node -p "require('./package.json').version")"
  if [[ "${version}" != "${current_version}" ]]; then
    ok "version bumped: ${C_DIM}${current_version} ${SYM_ARROW} ${version}${C_RESET}"
  else
    hint "version: ${C_DIM}${version}${C_RESET}"
  fi

  # Install deps if needed.
  if [[ ! -d "node_modules" || "package.json" -nt "node_modules" ]]; then
    pnpm install
  fi

  # Per-target build.
  [[ $target_chrome  -eq 1 ]] && pnpm -s build
  [[ $target_firefox -eq 1 ]] && pnpm -s build:firefox
  [[ $target_edge    -eq 1 ]] && pnpm -s build:edge
  [[ $target_opera   -eq 1 ]] && pnpm -s build:opera
  [[ $target_safari  -eq 1 ]] && pnpm -s build:safari

  # Purge prior-version zips before re-zipping so .output stays clean.
  rm -f .output/*.zip

  [[ $target_chrome  -eq 1 ]] && pnpm -s zip
  [[ $target_firefox -eq 1 ]] && pnpm -s zip:firefox
  [[ $target_edge    -eq 1 ]] && pnpm -s zip:edge
  [[ $target_opera   -eq 1 ]] && pnpm -s zip:opera
  [[ $target_safari  -eq 1 ]] && pnpm -s zip:safari

  # Optional Firefox signing.
  signed_xpi=""
  if [[ $do_sign -eq 1 && $target_firefox -eq 1 ]]; then
    rm -f web-ext-artifacts/*.xpi
    hint "signing Firefox with web-ext ${C_DIM}(1-2 min)${C_RESET}"
    spin "web-ext sign (Firefox)" web-ext sign \
      --source-dir .output/firefox-mv2 \
      --api-key="${WEB_EXT_API_KEY}" \
      --api-secret="${WEB_EXT_API_SECRET}" \
      --channel unlisted
    signed_xpi="$(ls -t web-ext-artifacts/*.xpi 2>/dev/null | head -1 || true)"
    if [[ -z "${signed_xpi}" ]]; then
      fail "signed .xpi not found in web-ext-artifacts/"
      exit 1
    fi
  fi

  # Collect artifacts.
  mkdir -p "${out_dir}"
  find "${out_dir}" -maxdepth 1 -type f \( -name 'weavemind-*-v*.zip' -o -name 'weavemind-*-v*.xpi' \) -delete

  src_prefix="weft-extension-${version}"
  copy_zip() {
    local target="$1"
    local label="$2"
    cp ".output/${src_prefix}-${target}.zip" "${out_dir}/weavemind-${target}-v${version}.zip"
    ok "weavemind-${target}-v${version}.zip ${C_DIM}(${label})${C_RESET}"
  }
  [[ $target_chrome  -eq 1 ]] && copy_zip "chrome"  "Chrome / Brave / Vivaldi / Arc"
  [[ $target_firefox -eq 1 ]] && copy_zip "firefox" "Firefox unsigned, load via about:debugging"
  [[ $target_edge    -eq 1 ]] && copy_zip "edge"    "Edge"
  [[ $target_opera   -eq 1 ]] && copy_zip "opera"   "Opera"
  [[ $target_safari  -eq 1 ]] && copy_zip "safari"  "Safari (needs xcrun on macOS)"

  if [[ -n "${signed_xpi}" ]]; then
    cp "${signed_xpi}" "${out_dir}/weavemind-firefox-v${version}.xpi"
    ok "weavemind-firefox-v${version}.xpi ${C_DIM}(Firefox signed)${C_RESET}"
  fi

  # Stable unversioned symlinks. Re-point or remove on every run.
  for t in chrome firefox edge opera safari; do
    var="target_${t}"
    if [[ "${!var}" -eq 1 ]]; then
      ln -sf "weavemind-${t}-v${version}.zip" "${out_dir}/weavemind-${t}.zip"
    else
      rm -f "${out_dir}/weavemind-${t}.zip"
    fi
  done
  if [[ -n "${signed_xpi}" ]]; then
    ln -sf "weavemind-firefox-v${version}.xpi" "${out_dir}/weavemind-firefox.xpi"
  else
    rm -f "${out_dir}/weavemind-firefox.xpi"
  fi

  popd >/dev/null

  ok "browser extension v${version} ready in ${C_DIM}${out_dir}/${C_RESET}"
fi

# ---- post-install summary --------------------------------------------
#
# Print this once per default install (CLI + daemon + VS Code). It's
# the first thing a fresh user sees that tells them what to actually
# DO with the install they just ran.

is_default_install=0
if [[ $build_cli -eq 1 && $refresh_daemon -eq 1 && $build_vscode -eq 1 && $build_browser -eq 0 ]]; then
  is_default_install=1
fi

if [[ $build_cli -eq 1 ]]; then
  case ":${PATH}:" in
    *":${bin_dir}:"*)
      path_ok=1
      ;;
    *)
      path_ok=0
      ;;
  esac

  if [[ $is_default_install -eq 1 ]]; then
    rule="$(printf '%.0s─' $(seq 1 64))"
    printf '\n%s%s%s\n' "${C_DIM}" "${rule}" "${C_RESET}"
    printf '%s%s%s %s%sSetup complete.%s\n\n' "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${C_BOLD}" "${C_BLUE}" "${C_RESET}"

    printf '%s%sRunning:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
    printf '  %s%s%s dispatcher  ${C_DIM}%s${C_RESET}  ${C_DIM}(kind cluster, weft-system ns)${C_RESET}\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "http://127.0.0.1:9999"
    printf '  %s%s%s postgres    ${C_DIM}in-cluster, durable across daemon restarts${C_RESET}\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}"
    printf '  %s%s%s VS Code     ${C_DIM}installed (reload your window if it is open)${C_RESET}\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}"

    printf '\n%s%sTry it out:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
    printf '  %s%s%s sanity-check       %sweft daemon status%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %s%s%s scaffold + run     %sweft new my-first-weave && cd my-first-weave && weft run%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %s%s%s open dashboard     %shttp://127.0.0.1:9999%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"

    printf '\n%s%sBrowser extension%s ${C_DIM}(HumanQuery / in-page weaves)${C_RESET}\n' \
      "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
    printf '  %sbuild it once:%s     %s./setup.sh --browser --no-sign --no-bump%s\n' \
      "${C_DIM}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %sthen load %sextension-build/weavemind-chrome.zip%s ${C_DIM}via chrome://extensions${C_RESET}\n' \
      "${C_DIM}" "${C_RESET}${C_BOLD}" "${C_RESET}"

    printf '\n%s%sDay-to-day:%s\n' "${C_BOLD}" "${C_BLUE}" "${C_RESET}"
    printf '  %s%s%s tail logs          %sweft daemon logs --tail 200 -f%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %s%s%s restart            %sweft daemon restart%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '  %s%s%s wipe everything    %s./setup.sh --uninstall --purge%s\n' \
      "${C_CYAN}" "${SYM_ARROW}" "${C_RESET}" "${C_BOLD}" "${C_RESET}"
    printf '%s%s%s\n' "${C_DIM}" "${rule}" "${C_RESET}"
  fi

  if [[ $path_ok -eq 1 ]]; then
    printf '\n%s%s%s %s is on PATH. try: %sweft --help%s\n' \
      "${C_GREEN}" "${SYM_OK}" "${C_RESET}" "${bin_dir}" "${C_BOLD}" "${C_RESET}"
  else
    printf '\n%s%s%s %s is NOT on your PATH; add to your shell rc:\n' \
      "${C_YELLOW}" "${SYM_WARN}" "${C_RESET}" "${bin_dir}"
    printf '    %sexport PATH="%s:$PATH"%s\n' "${C_BOLD}" "${bin_dir}" "${C_RESET}"
  fi
fi
