# Tangle for Devin Desktop

The template that installs Tangle on a weft project when the user opens
it in Devin Desktop. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Devin Desktop; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

Devin Desktop is what Windsurf became when Cognition renamed it in June 2026,
which is why the paths are split: rules live under `.devin/`, while skills,
workflows and hooks are still read from `.windsurf/`.

The persona is split across `.devin/rules/tangle-1.md` through `tangle-4.md`,
each `trigger: always_on`. **That split is not cosmetic:** Devin caps a
workspace rule file at 12,000 characters, and Tangle is about 29,000, so a
single file would be silently truncated. The split falls on section
boundaries, so each file is whole ideas rather than a cut sentence.

**No subagent file format exists here**, so the five specialists are skills
Tangle loads and becomes, exactly as in the Cline copy, with the same honest
note at the top of each about what that costs.

**The post-edit loop is the sharpest gap of any assistant here, and it is
documented as impossible rather than merely missing:** Devin's post-hooks
cannot block and cannot speak back into the model's context. So the hook
(`.windsurf/hooks/validate_weft.py`) leaves its answer on disk instead. It
writes findings to `.weft/validate-findings.txt` and **deletes that file when
the program is clean**, which matters as much as writing it: a stale file
would send Tangle chasing an error that no longer exists. The persona tells
Tangle to read that file after a batch of edits, which is what closes the loop
by hand.

The five commands are workflows in `.windsurf/workflows/`, invoked as
`/weft-check` and so on, and they are manual-only: Cascade never runs a
workflow on its own.

## What is here

Tangle is one persona plus eleven skills, five specialists and five commands.
The persona holds the program: the graph shape, the typed contracts, the weft
source. The skills are the big knowledge, loaded only when the work calls for
them. The specialists take the heavy scoped jobs. The commands are the loop's
steps, on demand, for a user who wants to drive.

The whole architecture, and why the pipeline verifies itself at both seams,
is written out once in
[`../claude-code/README.md`](../claude-code/README.md). This file covers only
what is different here.

## This copy is yours to tune

The template directories under `tangle/` are independent copies on purpose.
They start identical and are meant to drift: a wording this assistant answers
better to belongs in this folder and nowhere else. For what stays shared and
what may diverge, go and read
[`../README.md`](../README.md).
