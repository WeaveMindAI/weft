# Tangle for Devin Desktop

The template that installs Tangle on a weft project when the user opens
it in Devin Desktop. Tangle is not a product: it is the persona and the knowledge
these files install on whatever coding assistant loads them. Here that
assistant is Devin Desktop; sibling folders under `tangle/` carry the same Tangle
for other assistants.

## What is different here

Devin Desktop is what Windsurf became when Cognition renamed it in June 2026,
and its agent is Devin Local, which took over from Cascade. These files are
written for Devin Local. Rules, specialists, the hook and the permissions live
under `.devin/`; the skills are still read from `.windsurf/skills/`, which
Devin Local reads beside `.devin/skills/`.

The persona is split across `.devin/rules/tangle-1.md` through `tangle-8.md`,
each `trigger: always_on`. The Desktop docs cap a workspace rule file at 12,000 characters (the rules page that states it was written for Cascade, and the Devin Local rules page names no limit), and Tangle is over 50,000, so the persona stays in files under that cap. The split falls between
sections, or between numbered steps where a section is too long for one file
(the section "The loop" runs across `tangle-2.md` and `tangle-3.md`), so no file ends on a
cut sentence.

**The specialists are Devin subagents**, one profile per file in
`.devin/agents/`. They need subagents turned on (the Subagents (Preview) toggle in Devin Settings, `subagents_enabled` in `~/.config/devin/config.json`);
with it off, Tangle says so once and does the specialist's job itself from the
same file. A profile with no `model:` runs on Devin's default subagent model,
not on the model Tangle runs on. Tangle starts every specialist in the
background, and that is what keeps one from waiting on a permission prompt: a
background subagent cannot prompt, so when one of its commands is not on
the allow list in `.devin/config.json` and was not granted earlier in the session, Devin refuses it at once. So the list hands the specialists what their jobs need up front: writing in the folders the specialists own (`src/`, `nodes/`, `assets/`, `front/`, `design-lab/`, `.scratch/`, and `.gitignore`), since Devin's default mode asks before every write and a background subagent cannot answer; nothing under `.devin/` is on it, so no specialist can change the allow list or the hook, and a write there by Tangle puts a prompt in front of the user, the `weft` verbs Tangle runs while building plus the deployer's reads, `pnpm` (scripts run through `pnpm exec`), `node --env-file=.env build` and `kill`. Devin has no permission list per specialist, so these hold for all of them; the read-only ones simply have no tool that writes a file. Anything else comes back BLOCKED, and Tangle runs it so the user sees its prompt (a deployer's goes to the user instead, since it acts on a cloud install).

Three things Devin does not do here, and what the files do instead:

- **No message into a running subagent.** The frontend-builder's brief is a
  file, `front/BRIEF.md`, that Tangle amends and the frontend-builder reads
  again before each page. A finished specialist can be brought back with a
  new prompt, and Devin resumes it where it left off, but in the foreground,
  so Tangle does that only for a short follow-up and starts a fresh one in
  the background for anything longer.
- **No subagent starting a subagent**: none of these profiles opts in to it
  (`max-nesting`), so Tangle starts the `designer` in the same breath as the
  `frontend-builder`, and the two meet through `design-lab/`. The designer writes `design-lab/HANDOVER.md` last. If the frontend-builder is still running when that file appears, it moves the design in itself. If the frontend-builder finished first, Tangle starts a fresh one in the background, carrying `front/`, to do it.
- **No timer.** Devin tells Tangle when a specialist finishes, never when it is stuck. So at the start of each of its own turns, Tangle checks once on every specialist it started, or last checked, twenty minutes ago or more: the files the specialist changed in the last ten minutes, and if it changed none, or it is one that writes no files (the run-digger, the red-teamer, the deployer), the
  user's view of Devin's subagent list.

Once the node that will send a prompt is wired, the prompt-engineer tries that prompt on two real inputs through the node, with `weft run --from ... --feed ... --target`, on the program's
own model, because Devin has no command that sends a prompt alone to a model
of your choosing with no tools.

**The post-edit loop** is a `PostToolUse` hook (`.devin/hooks.v1.json`
running `.devin/hooks/validate_weft.py`) on `write`, `edit` and
`apply_patch`. It runs `weft validate` when the edit touched a `.weft` file
or anything under `nodes/`, and hands the findings back as the hook's
`additionalContext`, so they land in Tangle's context right after the edit.
Devin documents the edit tools' names but not the fields of their input, so
the hook reads only the keys named like a path (`path`, `file_path`,
`filePath`, `target_file`), plus the file headers of an `apply_patch` body,
so the text an edit writes is never mistaken for a path. It sees no edit made through
the terminal, and no hook runs in Restricted Mode, so the persona still has
Tangle run `weft validate` itself before anything runs or is handed over.

The seven commands are skills in `.windsurf/skills/` with `triggers: [user]`,
invoked as `/weft-check` and so on: Devin Local has no workflows, and
`triggers: [user]` keeps the agent from starting one of these skills on its own.

Devin Local keeps no memory between sessions. When the user asks Tangle to
work differently in a project, Tangle writes it into `.windsurf/rules/project.md`, with `trigger: always_on`. Devin Local loads `.windsurf/rules/*.md` as rules, and this template ships no `.windsurf/rules/`, so `weft tangle update` never touches it. If the user wants a permission kept, it goes in `.devin/config.local.json`, which the update leaves alone too.

## What is here

Tangle is one persona plus its reference skills, eight specialists and seven commands.
The persona holds the program: the graph shape, the typed contracts, the weft
source. The skills are the big knowledge, loaded only when the work calls for
them. The specialists take the heavy scoped jobs. The commands are the loop's
steps, on demand, for a user who wants to drive.

| Path | What it is |
|---|---|
| `.devin/rules/tangle-1.md` to `tangle-8.md` | the persona, always on |
| `.devin/agents/group-builder.md` | builds the inside of one included file under `src/` and proves it alone with `weft run --group` |
| `.devin/agents/node-smith.md` | builds and proves exactly one node, local test tiers green, live tests written but not run |
| `.devin/agents/frontend-builder.md` | builds and proves the frontend under `front/` from `front/BRIEF.md`, then moves the designer's work in |
| `.devin/agents/designer.md` | the site's look and motion in `design-lab/`, every page mocked, proven with screenshots and recordings |
| `.devin/agents/prompt-engineer.md` | writes the program's LLM prompts and tries each on two real inputs through the node that sends it, or hands it over with the trial still to do when that node is not wired yet |
| `.devin/agents/run-digger.md` | post-mortem only, read-only, never fixes |
| `.devin/agents/deployer.md` | the only agent that acts on a cloud install |
| `.devin/agents/red-teamer.md` | attack only, read-only, never fixes and never runs |
| `.devin/hooks.v1.json`, `.devin/hooks/validate_weft.py` | the compiler's answer after every edit |
| `.devin/config.json` | the allow list: writing in the folders the specialists own, the `weft` verbs Tangle and the specialists run, `pnpm`, `node --env-file=.env build`, `kill`; the daemon's lifecycle is left off |
| `.windsurf/skills/weft-*/` | the knowledge skills, each loaded when its description matches the work, and the seven commands |

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
