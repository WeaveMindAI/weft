#!/usr/bin/env bash
# Print a `-target=` argument for both ends of every `moved` block in the
# Terraform files of a directory, one per line, sorted and without repeats.
#
# A targeted `terraform apply` refuses a state where a `moved` block still
# has work to do unless both its `from` and its `to` are targeted ("Moved
# resource instances excluded by targeting"). The install workflow's
# targeted applies (deploy/terraform/gcp) add these, so an install last
# updated before any rename, of any age, goes through them. Targeting an
# address nothing is at (a `from` already moved, or never there on a new
# install) is no work for Terraform.
#
# The blocks are read the way Terraform reads them, not by line: one-line
# or spread over several lines, `#`, `//` and `/* */` comments, strings and
# heredocs (a `moved {` inside one is not a block), and any number of
# files. A block missing its `from` or its `to` is an error naming its file
# and line.
#
# Usage: scripts/terraform-moved-targets.sh <terraform-dir>
# Its test: scripts/terraform-moved-targets.test.sh
set -euo pipefail

if [[ $# -ne 1 || ! -d "$1" ]]; then
  echo "usage: scripts/terraform-moved-targets.sh <terraform-dir>" >&2
  exit 2
fi
shopt -s nullglob
files=("$1"/*.tf)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "no .tf files in $1" >&2
  exit 1
fi

awk '
  function flush() {
    if (word != "") { emit(word); word = "" }
  }
  function emit(t) {
    n++; tok[n] = t; at[n] = FILENAME ":" FNR
  }
  FNR == 1 { flush(); stack = ""; block = 0; heredoc = "" }
  {
    line = $0
    if (heredoc != "") {
      trimmed = line; gsub(/^[ \t]+|[ \t\r]+$/, "", trimmed)
      if (trimmed == heredoc) heredoc = ""
      next
    }
    len = length(line); i = 1
    while (i <= len) {
      c = substr(line, i, 1); two = substr(line, i, 2)
      if (block) {
        if (two == "*/") { block = 0; i += 2 } else i++
        continue
      }
      top = substr(stack, length(stack), 1)
      # Inside a string: everything is part of the word, and a template
      # (${ } or %{ }) may hold braces and strings of its own.
      if (top == "s") {
        word = word c
        if (c == "\\") { word = word substr(line, i + 1, 1); i += 2; continue }
        if (substr(line, i, 3) == "$${" || substr(line, i, 3) == "%%{") { word = word substr(line, i + 1, 2); i += 3; continue }
        if (two == "${" || two == "%{") { word = word "{"; stack = stack "i"; i += 2; continue }
        if (c == "\"") stack = substr(stack, 1, length(stack) - 1)
        i++
        continue
      }
      if (top == "i") {
        word = word c
        if (c == "\"") stack = stack "s"
        else if (c == "{") stack = stack "i"
        else if (c == "}") stack = substr(stack, 1, length(stack) - 1)
        i++
        continue
      }
      if (c == "#" || two == "//") break
      if (two == "/*") { flush(); block = 1; i += 2; continue }
      if (c == "\"") { word = word c; stack = stack "s"; i++; continue }
      if (two == "<<") {
        flush()
        rest = substr(line, i + 2); sub(/^-/, "", rest); sub(/[ \t\r]+$/, "", rest)
        heredoc = rest
        break
      }
      if (c == " " || c == "\t" || c == "\r") { flush(); i++; continue }
      if (c == "{" || c == "}" || c == "=" || c == "," || c == "(" || c == ")") { flush(); emit(c); i++; continue }
      word = word c
      i++
    }
    if (stack == "") flush()
  }
  END {
    flush()
    depth = 0; bad = 0
    for (k = 1; k <= n; k++) {
      t = tok[k]
      if (depth == 0 && t == "moved" && tok[k + 1] == "{") {
        where = at[k]; from = ""; to = ""; d = 1; k += 2
        for (; k <= n; k++) {
          t = tok[k]
          if (t == "{") d++
          else if (t == "}") { d--; if (d == 0) break }
          else if (d == 1 && (t == "from" || t == "to") && tok[k + 1] == "=") {
            if (t == "from") from = tok[k + 2]; else to = tok[k + 2]
            k += 2
          }
        }
        if (from == "" || to == "") {
          printf "%s: this moved block names no %s\n", where, (from == "" ? "from" : "to") > "/dev/stderr"
          bad = 1
          continue
        }
        print "-target=" from
        print "-target=" to
        continue
      }
      if (t == "{") depth++
      else if (t == "}") depth--
    }
    exit bad
  }
' "${files[@]}" | sort -u
