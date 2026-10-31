#!/usr/bin/env bash
# The test of scripts/terraform-moved-targets.sh: sample Terraform files in
# every shape a `moved` block comes in, and the real install's. CI runs it
# beside `terraform validate`.
#
# Usage: scripts/terraform-moved-targets.test.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
extract="${here}/terraform-moved-targets.sh"
scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT
failed=0

# check <name> <expected output>: run the extractor on ${scratch}/<name>.
check() {
  local got
  if ! got="$("${extract}" "${scratch}/$1" 2>&1)"; then
    echo "FAIL $1: the extractor failed: ${got}"
    failed=1
    return
  fi
  if [[ "${got}" != "$2" ]]; then
    printf 'FAIL %s\n  expected:\n%s\n  got:\n%s\n' "$1" "$2" "${got}"
    failed=1
    return
  fi
  echo "ok   $1"
}

mkdir -p "${scratch}/shapes"
cat >"${scratch}/shapes/a.tf" <<'EOF'
# moved { from = in.a_comment to = not.a_block }
// moved { from = in.another to = comment.style }
/* moved {
  from = in.a_block_comment
  to   = is.not.one
} */
resource "random_password" "install_key" {
  length = 48 # a comment holding a brace }
  description = "a string holding moved { from = x.y to = z.w } and a brace {"
  template = "${jsonencode({ moved = { from = "q" } })} and $${literal} and \" quote {"
  # A template whose expression is a string holding a closing brace.
  odd = "${"}"}"
}

moved {
  from = random_password.bootstrap_operator_key
  to   = random_password.install_key
}

moved { from = google_storage_bucket.one_line }
EOF
cat >"${scratch}/shapes/b.tf" <<'EOF'
locals {
  script = <<-EOT
    moved {
      from = in.a_heredoc
      to   = is.not.one
    }
    } a brace it does not open
  EOT
  moved = "an attribute named moved is not a block"
}

moved{
	to=module.files.google_storage_bucket.files["main bucket"]  # to first, tabs, no spaces
	from=google_storage_bucket.files
}

moved {
  # a comment inside the block
  from = google_storage_bucket.one_line
  to   = google_storage_bucket.renamed /* and one here */
}
EOF
# The one-line block in a.tf names only a `from`: the extractor must say so.
if out="$("${extract}" "${scratch}/shapes" 2>&1)"; then
  echo "FAIL shapes: a block with no to was accepted: ${out}"
  failed=1
elif [[ "${out}" != *"a.tf:20: this moved block names no to"* ]]; then
  echo "FAIL shapes: the refusal does not name the block: ${out}"
  failed=1
else
  echo "ok   a block with no to is refused, naming its file and line"
fi
sed -i 's/^moved { from = google_storage_bucket.one_line }$/moved { from = google_storage_bucket.one_line  to = google_storage_bucket.one_line_too }/' "${scratch}/shapes/a.tf"
check shapes "$(cat <<'EOF'
-target=google_storage_bucket.files
-target=google_storage_bucket.one_line
-target=google_storage_bucket.one_line_too
-target=google_storage_bucket.renamed
-target=module.files.google_storage_bucket.files["main bucket"]
-target=random_password.bootstrap_operator_key
-target=random_password.install_key
EOF
)"

mkdir -p "${scratch}/none"
cat >"${scratch}/none/main.tf" <<'EOF'
resource "random_id" "x" {
  byte_length = 8
}
EOF
check none ""

# The install's own files: whatever else they hold, the rename that broke
# targeted applies on installs made before it.
if real="$("${extract}" "${here}/../deploy/terraform/gcp")" \
  && grep -qx -- '-target=random_password.bootstrap_operator_key' <<<"${real}" \
  && grep -qx -- '-target=random_password.install_key' <<<"${real}"; then
  echo "ok   deploy/terraform/gcp"
else
  echo "FAIL deploy/terraform/gcp: got: ${real:-<nothing>}"
  failed=1
fi

exit "${failed}"
