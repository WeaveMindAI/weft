#!/usr/bin/env bash
# Delete every revision of each of weft's role services except the latest,
# once the latest is ready and gets all of its service's requests.
#
# Deleting a revision stops its copies: Cloud Run sends each SIGTERM, and
# the copy answers what it is answering, ends what it holds open (an event
# stream, a worker's line), gives back the tasks it runs, and exits. So the
# copies of the weft an update replaced end here, instead of living on
# beside the new ones for as long as a stream or a line keeps them up. The
# install workflow runs this right after the apply that rolled the new
# revisions out, which already waited for each to be ready.
#
# Every service is checked before anything is deleted: one whose latest
# revision is not ready, or gets less than all of its requests (traffic
# pinned or split by hand), is refused naming it, and nothing of any
# service is deleted. A deletion gcloud refuses part way stops the script
# with gcloud's error; running it again lists every service afresh and
# deletes what is left.
#
# Usage: scripts/delete-old-role-revisions.sh <project> <region> <service>...
# Its test: scripts/delete-old-role-revisions.test.sh
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: scripts/delete-old-role-revisions.sh <project> <region> <service>..." >&2
  exit 2
fi
project="$1"
region="$2"
shift 2

# old_revisions <service> <its description as JSON> <its revisions, one per
# line>: the revisions to delete, one per line, or a refusal naming the
# service on stderr.
old_revisions() {
  local service="$1" described="$2" listed="$3" latest ready serving
  latest="$(jq -r '.status.latestCreatedRevisionName // ""' <<<"${described}")"
  ready="$(jq -r '.status.latestReadyRevisionName // ""' <<<"${described}")"
  if [[ -z "${latest}" ]]; then
    echo "${service}: Cloud Run names no latest revision for it" >&2
    return 1
  fi
  if [[ "${ready}" != "${latest}" ]]; then
    echo "${service}: its latest revision ${latest} is not ready (the latest ready one is ${ready:-none}); its log says why" >&2
    return 1
  fi
  serving="$(jq -r --arg latest "${latest}" '[.status.traffic[]? | select(.revisionName == $latest) | .percent // 0] | add // 0' <<<"${described}")"
  if [[ "${serving}" != 100 ]]; then
    echo "${service}: its latest revision ${latest} gets ${serving}% of its requests, not all of them (traffic pinned or split by hand); send them all to it (gcloud run services update-traffic ${service} --to-latest) and run this again" >&2
    return 1
  fi
  grep -v -x -F -e "${latest}" <<<"${listed}" | sed '/^$/d' || true
}

# Every service checked first, so a refusal deletes nothing.
declare -A going
for service in "$@"; do
  described="$(gcloud run services describe "${service}" --project "${project}" --region "${region}" --format=json)"
  listed="$(gcloud run revisions list --service "${service}" --project "${project}" --region "${region}" --format='value(metadata.name)')"
  going["${service}"]="$(old_revisions "${service}" "${described}" "${listed}")"
done

for service in "$@"; do
  if [[ -z "${going[${service}]}" ]]; then
    echo "${service}: no older revision"
    continue
  fi
  while IFS= read -r revision; do
    echo "${service}: deleting ${revision}, which stops its copies"
    gcloud run revisions delete "${revision}" --project "${project}" --region "${region}" --quiet
  done <<<"${going[${service}]}"
done
