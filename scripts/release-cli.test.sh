#!/usr/bin/env bash
# The test of scripts/release-cli.sh: where it looks for a CLI built from
# a checkout's source, and in what order (the install first, then weft's
# release, then nothing, so the caller builds), with every address
# answered by a fake `curl` from files. CI runs it beside the
# moved-targets test.
#
# Usage: scripts/release-cli.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lookup="${here}/release-cli.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
failed=0

# A checkout with one commit, whose tree is what every source is asked for.
checkout="${scratch}/checkout"
git init -q "${checkout}"
echo weft >"${checkout}/README"
git -C "${checkout}" add README
git -C "${checkout}" -c user.name=t -c user.email=t@t commit -q -m one
tree="$(git -C "${checkout}" rev-parse 'HEAD^{tree}')"

# The fake web: an address's answer is the files named after it in
# ${web} (`<name>.status`, 200 when absent, and `<name>.body`), and an
# address with neither answers 404. Every address asked is logged.
web="${scratch}/web"
bin="${scratch}/bin"
mkdir -p "${bin}"
cat >"${bin}/curl" <<'EOF'
#!/usr/bin/env bash
url="" out="" format="" fail=0
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift ;;
    -w) format="$2"; shift ;;
    -H|--max-time|--retry) shift ;;
    -f|-fsSL|-fsS) fail=1 ;;
    http*) url="$1" ;;
  esac
  shift
done
echo "${url}" >>"${FAKE_WEB}/asked"
name="$(printf '%s' "${url}" | tr -c 'A-Za-z0-9' '_')"
status=404
[ -e "${FAKE_WEB}/${name}.body" ] && status=200
[ -e "${FAKE_WEB}/${name}.status" ] && status="$(cat "${FAKE_WEB}/${name}.status")"
if [ "${fail}" = 1 ] && [ "${status}" -ge 400 ]; then
  exit 22
fi
body="${FAKE_WEB}/${name}.body"
[ -e "${body}" ] || body=/dev/null
if [ -n "${out}" ]; then cat "${body}" >"${out}"; else cat "${body}"; fi
[ -n "${format}" ] && printf '%s' "${format//%\{http_code\}/${status}}"
exit 0
EOF
chmod +x "${bin}/curl"

install=https://install.example
release=https://github.com/WeaveMindAI/weft/releases/download/latest

# serve <url> <body file or text> [<status>]
serve() {
  local name
  name="$(printf '%s' "$1" | tr -c 'A-Za-z0-9' '_')"
  if [ -f "$2" ]; then cp "$2" "${web}/${name}.body"; else printf '%s' "$2" >"${web}/${name}.body"; fi
  [ -z "${3:-}" ] || echo "$3" >"${web}/${name}.status"
}

# A fresh web holding two different CLIs: the install's and the release's.
reset_web() {
  rm -rf "${web}"
  mkdir -p "${web}"
  echo "the install's cli" >"${scratch}/install-cli"
  echo "the release's cli" >"${scratch}/release-cli"
  install_sha="$(sha256sum "${scratch}/install-cli" | cut -d' ' -f1)"
  release_sha="$(sha256sum "${scratch}/release-cli" | cut -d' ' -f1)"
}

install_has_it() {
  serve "${install}/install/cli/${tree}" "{\"url\":\"https://bucket.example/cli\",\"sha256\":\"${1:-${install_sha}}\"}"
  serve https://bucket.example/cli "${scratch}/install-cli"
}

release_has_it() {
  serve "${release}/manifest.json" "{\"commit\":\"c0ffee\",\"tree\":\"${1:-${tree}}\",\"sha256_weft-x86_64-linux\":\"${release_sha}\"}"
  serve "${release}/weft-x86_64-linux" "${scratch}/release-cli"
}

# check <name> <expected exit> <expected CLI or -> <expected first address
# asked> [<install-url>]: run the lookup into a fresh folder.
check() {
  local dest="${scratch}/dest-$1" code=0 got first
  PATH="${bin}:${PATH}" FAKE_WEB="${web}" WEFT_OPERATOR_KEY=key \
    "${lookup}" "${checkout}" "${dest}" ${5:+"$5"} >"${scratch}/out" 2>&1 || code=$?
  got="$(cat "${dest}/weft" 2>/dev/null || echo -)"
  first="$(head -n 1 "${web}/asked" 2>/dev/null || echo -)"
  if [ "${code}" != "$2" ] || [ "${got}" != "$3" ] || [ "${first}" != "$4" ]; then
    printf 'FAIL %s\n  expected: exit %s, cli %s, first asked %s\n  got:      exit %s, cli %s, first asked %s\n  its output:\n%s\n' \
      "$1" "$2" "$3" "$4" "${code}" "${got}" "${first}" "$(cat "${scratch}/out")"
    failed=1
    return
  fi
  echo "ok   $1"
}

reset_web; install_has_it; release_has_it
check "the install's CLI comes first" 0 "the install's cli" "${install}/install/cli/${tree}" "${install}"
if grep -q "${release}" "${web}/asked"; then
  echo "FAIL the release was asked although the install had the CLI"
  failed=1
fi

reset_web; release_has_it
check "an install with none falls to the release" 0 "the release's cli" "${install}/install/cli/${tree}" "${install}"

reset_web; install_has_it; release_has_it
check "with no install named, the release is asked first" 0 "the release's cli" "${release}/manifest.json"

reset_web; release_has_it 0000000000000000000000000000000000000000
check "neither has it: the caller builds" 2 - "${install}/install/cli/${tree}" "${install}"

reset_web; install_has_it 0000000000000000000000000000000000000000000000000000000000000000; release_has_it
check "an install CLI that fails its sha256 is an error" 1 - "${install}/install/cli/${tree}" "${install}"

reset_web; release_has_it; serve "${install}/install/cli/${tree}" "the install is down" 503
check "an install that answers neither the CLI nor 404 is an error" 1 - "${install}/install/cli/${tree}" "${install}"

exit "${failed}"
