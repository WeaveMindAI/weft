# Design principles

What weft is for, stated as the properties every design decision is measured
against. When a proposal is hard to judge, judge it here.

## 0. What is actually being coordinated

Weft is usually described as a language for AI systems. True, and not the
description that helps you predict what it does.

A node has typed input ports, typed output ports, and a body that takes some
amount of time to answer. That is the entire contract, and nothing in it says
what is behind the body.

So `LlmInference` and `HumanQuery` are the same kind of thing to the compiler.
So is a Postgres query, a WhatsApp bridge holding a phone session, and a
long-lived agent that talks back over a channel for hours. All of them are
wired, checked and resumed the same way.

The durable primitives were not built for models. They were built for **steps
that take an unpredictable amount of time and might not answer**, which
describes a person and a service as exactly as it describes a model. Which is
why a weft program made only of people and services is an ordinary weft
program.

So what weft is for: **building one intelligent program
out of many processes and many intelligences, some human, some not.** Whether a
given participant is a person, a model, or a plain service is a detail at the
port.

## 1. The vocabulary is dynamic

Weft is not a catalog of integrations. The nodes that ship are examples and
conveniences. The language's job is to be expressive enough that whatever is
missing gets built on demand, correctly, by whoever needs it.

So the test of a new capability is never "did we add the node for X". It is:
**can the next X be expressed without touching the engine?**

Slack's held socket, Google's expiring watch channels and email's held IMAP
pipe are three different event topologies, and all three are declared data over
one engine. That is the bar. A feature that only works because weft
learned about a specific provider has failed it.

In force everywhere:

- Per-service knowledge lives in declared data (a service recipe, a node's
  metadata), never in engine, listener, broker, or dispatcher code.
- The escape hatch, when a mechanism genuinely cannot be data (a signature
  algorithm, a browser picker handshake), is a **closed typed variant** added
  deliberately and shared by every service. Never a per-service hook.
- The goal is expressive coverage. How many integrations ship in the box is
  not the measure.

## 2. The author is a model with a prompt, not a human with a manual

Weft's surfaces are written by a model working from a prompt and these docs.
That inverts the usual design pressure.

**Clarity is the bar; simplicity is not.** A surface may be large and detailed
if it is legible and steers toward the correct shape. "Would a newcomer find
this easy" is the wrong test. The right test is: given this vocabulary and
these docs, does the model build the right thing?

So: name concepts for what they mean, state invariants where they are enforced,
make the doc comment say **why** a rule exists, and prefer one honest complex
declaration over several simple ones that have to be kept in agreement by hand.

A builder that hides a required decision, or a default that silently picks a
policy, reads as simplicity and is not: the model fills it in wrong and nothing
complains.

## 3. The right shape is the easy shape; wrong shapes are refused

Three kinds of mistake, with three intended fates.

**Structurally impossible.** The best outcome, where the vocabulary cannot
express the mistake at all.

**Refused loudly, naming the fix.** At parse, at compile, or at registration,
before anything runs. Most rules land here.

**Runs and silently misbehaves.** Forbidden. A mistake that can reach this
bucket is a design defect to fix.

What that rules out is the trigger that registers fine and never fires, or the
filter that matches nothing forever. Each looks healthy from every angle you
can see.

So every declared surface validates its own internal consistency at the moment
it enters the system, and says what is missing in the words of the fix:

- a subscription that renews but records no expiry,
- a recipe that routes by an id it never mints,
- a check that compares against a secret nobody sends,
- a filter whose pattern does not compile,
- a service offering a door its own auth cannot serve.

All of these are refused at registration.

### The corollary: no fallbacks

A path that "works anyway" hides the defect. Either it works correctly, or it
fails with a message that names the recovery.

Failing loudly is not enough on its own. For each piece of state a failed
operation created, two questions: can the user act on it (resume, retry,
inspect, or delete through a documented action), and does it hold value the
recovery would need? If neither, clean it up. Nothing may remain that the user
can neither act on nor delete and does not know about.

## 4. The system supplies the recipes, so bodies stay logic

The declared surface exists to make correct construction the default.

A node body contains its own logic and nothing else. Everything around it,
transport, credentials, acknowledgement, subscription lifecycle, retries, is
the language's, implemented once. Where exactly that line falls today:
[the commandments of plumbing](plumbing.md).

The practical rule when adding to the node-facing surface: **if three calls are
always implied by one intent, expose the one call.** Models improvise across
chained calls.

The reverse rule matters just as much. A node embodies **one** user
expectation, so when one capability answers two different questions, build two
nodes even when the machinery beneath is identical.

## How these show up in the code

**One central mapping per concept.** One place maps a type to an editor widget.
One place maps a mime type to a file primitive. One place decides which
transport serves an event topic. A second place would drift.

**Naming by contract, not mechanism.** A function is named for what the caller
receives, never for how it does it. Caching, pooling, and where the data comes
from belong in the doc comment.

**Comment headers carry the why.** Nearly every file opens with prose stating
its responsibility and the failure mode a decision avoids.

**Cross-language invariants are pinned.** Where one concept genuinely must
exist in two languages, every definition site carries a marker naming every
other site, so changing one surfaces the rest.

---

If you want to know more about our approach to AI safety, it is
[here](safety.md).
