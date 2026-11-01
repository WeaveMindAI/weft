#!/usr/bin/env bash
# The test of scripts/delete-old-role-revisions.sh: which revisions it
# deletes, and which services it refuses, with Cloud Run answered by a fake
# `gcloud` from files. CI runs it beside the moved-targets test.
#
# Usage: scripts/delete-old-role-revisions.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="${here}/delete-old-role-revisions.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
failed=0

# The fake Cloud Run: a service's description is ${cloud}/<service>.json,
# its revisions ${cloud}/<service>.revisions, and every revision deleted
# is logged to ${cloud}/deleted as `<revision> <project> <region>`.
cloud="${scratch}/cloud"
bin="${scratch}/bin"
mkdir -p "${cloud}" "${bin}"
cat >"${bin}/gcloud" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cloud="${cloud}"
EOF
cat >>"${bin}/gcloud" <<'EOF'
flag() {
  local name="$1"
  shift
  while [[ $# -gt 0 ]]; do
    if [[ "$1" == "${name}" ]]; then
      echo "$2"
      return
    fi
    shift
  done
}
case "$1 $2 $3" in
  "run services describe") cat "${cloud}/$4.json" ;;
  "run revisions list") cat "${cloud}/$(flag --service "$@").revisions" ;;
  "run revisions delete") echo "$4 $(flag --project "$@") $(flag --region "$@")" >>"${cloud}/deleted" ;;
  *) echo "the fake gcloud does not know: $*" >&2; exit 1 ;;
esac
EOF
chmod +x "${bin}/gcloud"

# service <name> <latest created> <latest ready> <traffic JSON> <revisions...>
service() {
  local name="$1" latest="$2" ready="$3" traffic="$4"
  shift 4
  jq -n --arg latest "${latest}" --arg ready "${ready}" --argjson traffic "${traffic}" \
    '{status: {latestCreatedRevisionName: $latest, latestReadyRevisionName: $ready, traffic: $traffic}}' >"${cloud}/${name}.json"
  printf '%s\n' "$@" >"${cloud}/${name}.revisions"
}

# check <name> <expected exit> <expected deletions> <expected stderr part> <services...>
check() {
  local name="$1" exit_wanted="$2" deleted_wanted="$3" said_wanted="$4" code=0 deleted said
  shift 4
  rm -f "${cloud}/deleted"
  said="$(PATH="${bin}:${PATH}" "${script}" proj reg "$@" 2>&1 >/dev/null)" || code=$?
  deleted="$(cat "${cloud}/deleted" 2>/dev/null || true)"
  if [[ "${code}" != "${exit_wanted}" || "${deleted}" != "${deleted_wanted}" || "${said}" != *"${said_wanted}"* ]]; then
    printf 'FAIL %s\n  exit %s (wanted %s)\n  deleted:\n%s\n  wanted:\n%s\n  said: %s\n  wanted it to hold: %s\n' \
      "${name}" "${code}" "${exit_wanted}" "${deleted}" "${deleted_wanted}" "${said}" "${said_wanted}"
    failed=1
    return
  fi
  echo "ok   ${name}"
}

all='[{"revisionName": "r-3", "percent": 100, "latestRevision": true}]'

# The latest ready and serving everything: every other revision goes,
# service by service.
service weft-role-dispatcher r-3 r-3 "${all}" r-1 r-2 r-3
service weft-role-broker b-7 b-7 '[{"revisionName": "b-7", "percent": 100, "latestRevision": true}]' b-6 b-7
check "every older revision goes" 0 "$(printf '%s\n' 'r-1 proj reg' 'r-2 proj reg' 'b-6 proj reg')" "" weft-role-dispatcher weft-role-broker

# Nothing older: nothing goes.
service weft-role-listener l-1 l-1 '[{"revisionName": "l-1", "percent": 100, "latestRevision": true}]' l-1
check "a service with one revision" 0 "" "" weft-role-listener

# Traffic named by revision rather than following the latest counts the same.
service weft-role-supervisor s-2 s-2 '[{"revisionName": "s-2", "percent": 100}]' s-1 s-2
check "traffic pinned to the latest" 0 "s-1 proj reg" "" weft-role-supervisor

# The latest not ready: refused naming the service, and nothing of any
# service goes, even one checked before it.
service weft-role-broker b-8 b-7 '[{"revisionName": "b-7", "percent": 100, "latestRevision": true}]' b-6 b-7 b-8
check "the latest is not ready" 1 "" "weft-role-broker: its latest revision b-8 is not ready" weft-role-dispatcher weft-role-broker

# The latest ready but sharing the requests with an older one: refused.
service weft-role-broker b-8 b-8 '[{"revisionName": "b-7", "percent": 50}, {"revisionName": "b-8", "percent": 50}]' b-7 b-8
check "the latest gets half" 1 "" "weft-role-broker: its latest revision b-8 gets 50% of its requests" weft-role-broker

# Traffic pinned to an older revision (a rollback by hand): refused.
service weft-role-broker b-8 b-8 '[{"revisionName": "b-7", "percent": 100}]' b-7 b-8
check "traffic pinned to an older one" 1 "" "weft-role-broker: its latest revision b-8 gets 0% of its requests" weft-role-broker

exit "${failed}"
