---
name: mode-writing
description: "Writing mode: distill prose, fight length and jargon, loop drafts through the AI-tell catalog until a full pass finds nothing. Load when switching to [writing mode] or before writing any substantial prose (docs, READMEs, design notes, journal entries, long replies)."
---

Applies to prose deliverables: docs, journal entries, READMEs, design notes, release writeups, substantial below-the-line replies. Not quick answers.

The two chronic weaknesses are length and jargon: three paragraphs where five words land harder, and invented vocabulary. Distill: find the essence, say that, stop. A few words are usually more impactful than a few paragraphs. The first draft is never the output.

The loop, per section, repeated until a full pass finds nothing:

1. Write the draft.
2. Mark what is imprecise, unclear, or too long.
3. Make every sentence fight for its life: cover it, read the paragraph without it, put it back only if something was lost. Length is a defect. The failing shapes: repeats the sentence before it, lands a beat, bolts a clause onto a finished sentence, pads a list to three, explains what the reader already got, spells out what was implied, states a general truth instead of a fact. Run this on the pages you're happy with too; that's where padding survives.
4. Say every surviving sentence the simple way. Never a harder phrasing where a plain one exists: "a node does one thing", not "a node is one capability, sharply scoped". The tells: an abstract noun doing a verb's job, any phrase the reader must stop and decode. Test: would you say it to somebody next to you, in a hurry?
5. Reformulate so the right image lands with no effort.
6. Rewrite until it is enjoyable to read.
7. AI-tell pass: red team against the catalog below, quote every offending phrase, no defending instances. A phrase that matches a pattern gets fixed.

**The catalog:**

*Structure and rhythm*
- Contrast mirrors ("X, not Y"): "just the shape, not legal language". Fix: state X plainly.
- Rule-of-three triads: "faster, cheaper, and more reliable". Fix: keep the item that matters, or let the list be two or four.
- "It's not just X, it's Y". Fix: say what it is.
- Dramatic one-line punches: "That's exactly when it matters." Fix: merge or cut.
- Uniform bullet weight: every bullet a polished aphorism of equal length. Fix: let length follow content; a bullet can be one sentence or several, never a clipped fragment for style.
- Perfectly parallel openers across paragraphs. Fix: vary or restructure.
- Question-as-transition: "So what does this mean for you?" Fix: continue.
- Staccato drama: "Cut. Rewrite. Repeat." Fix: normal-length sentences; long is fine when that's how the thought runs.

*Register and voice*
- Performative sincerity: "honestly", "to be clear", "let me be direct". Fix: delete, just say the thing.
- Grand framing: "The principle:", "Here's the thing:", "The bottom line:". Fix: start with the content.
- Profound closers: "Then we make it real." Fix: end on the last useful sentence.
- Clever-quip register in serious text. Fix: plain statement.
- False humility: "in my humble opinion", "I could be wrong but". Fix: state it; add real uncertainty only if it exists.
- Narrating structure: "In this section we'll explore". Fix: explore it.

*Word-level tells*
- Stock intensifiers: "truly", "deeply", "genuinely", "incredibly", "remarkably". Fix: cut, or a concrete detail.
- LLM darlings: "delve", "landscape", "tapestry", "journey", "unlock", "leverage", "elevate", "seamless", "robust", "holistic", "navigate" (metaphorical), "embark", "foster", "crucial", "pivotal", "vibrant", "testament to", "underscores", "resonates". Fix: plain synonym.
- Corporate jargon: "synergize", "ecosystem" (non-technical), "value-add", "circle back", "double-click on". Fix: plain language.
- Empty amplifiers: "powerful insights", "meaningful impact", "compelling narrative". Fix: the noun alone, or a specific claim.
- Hedging stacks: "might potentially be able to". Fix: one hedge max.

*Content-level tells*
- Both-sides-ism where a position is warranted. Fix: take the position.
- Summarizing what was just said. Fix: cut the summary.
- Caveat paragraphs nobody asked for. Fix: cut, or one clause.
- Restating the prompt before answering. Fix: answer.
- Softening every claim to universal agreeability. Fix: keep the sharp version.
- Announcing the test: "your reaction will tell me a lot". Fix: state the ask, keep the evaluation to yourself; a reaction explained-for is no longer informative.
- Editing artifacts: arguing with a version of itself the reader never saw ("that thing is not X by itself"). Fix: state the claim directly.

Patterns compound: a triad inside a punch sentence is three findings, not one.

The em dash ban in `.codex/config.toml` applies to everything you write here.