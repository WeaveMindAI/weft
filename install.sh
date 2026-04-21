#!/usr/bin/env bash
# install.sh — build the weft CLI + dispatcher + runner in release
# mode and symlink them into ~/.local/bin so `weft` is on PATH
# without extra env vars.
#
# Usage:
#   ./install.sh                  # build + install
#   ./install.sh --debug          # use debug profile (faster build)
#   ./install.sh --prefix /path   # install into /path/bin instead
#   ./install.sh --uninstall      # remove the symlinks

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
profile="release"
prefix="${HOME}/.local"
uninstall=0

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
    -h|--help)
      cat <<EOF
Usage: ./install.sh [--debug] [--prefix PATH] [--uninstall]

Builds weft / weft-dispatcher / weft-runner and symlinks them to
PREFIX/bin (default: ~/.local/bin).

  --debug       Build with the debug profile (faster, larger binaries)
  --prefix PATH Install into PATH/bin instead of ~/.local/bin
  --uninstall   Remove the installed symlinks (no build)
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
mkdir -p "${bin_dir}"

binaries=(weft weft-dispatcher weft-runner)

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
echo "done. binaries live under ${bin_dir}."

# PATH check.
case ":${PATH}:" in
  *":${bin_dir}:"*)
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
