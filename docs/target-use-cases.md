# Target Use Cases

Concrete systems we want Weft to express cleanly, written from the
outside in. Each entry describes the system in its ideal fully-in-Weft
form, then names the language primitives that form requires. This is
the dual of `TODO.md`: `TODO.md` lists deferred language work; this
doc is the forcing function for it. A use case here is evidence that a
deferred primitive is the right one, and occasionally it surfaces a
primitive `TODO.md` doesn't yet name.

Entries are not commitments to build now. They are the targets the
language grows toward. Where a use case ships before the language can
express it cleanly, the entry says so and records the interim shape, so
the compromise is visible and revisited when the primitives land.

## Real-time voice agent (tight conversational loop)

**The use case.** A voice agent that answers a live call, holds a
real-time spoken conversation (transcribe, decide, speak, repeat) at
human-conversational latency, and when the call ends hands off to a
durable workflow that can span days (capture a record, wait for a human
approval, call back later, write a compliance artefact). The
conversation is real-time and ephemeral; everything after the call is
durable and journaled.

**Gold shape (the target).** The conversation itself runs in Weft. Only
two thin pieces stay as infrastructure: a transport that detects an
incoming call and pipes audio, and the model hosting (STT / LLM / TTS
served locally). Everything else is in the graph and editable:

- An incoming call fires a trigger.
- Transcription runs as a long-lived node that streams partial
  utterances as they are recognised, while still running.
- Each utterance flows through decision logic (classify, branch,
  detect keywords) and back out to speech, in a loop that runs until
  the caller disconnects.
- Intermediate emissions fan out in parallel: while the conversation
  continues, downstream work (filling a form, detecting a condition,
  starting a sibling workflow) runs concurrently off partial results.
- When the call ends, the loop closes and emits a structured result
  that begins the durable post-call workflow.

The point of the gold shape is that the conversational behaviour (what
the agent says, when it escalates, how it branches) is in-graph,
visible, and editable by a vertical builder, not buried in opaque
sidecar code.

**Primitives the gold shape requires.**

- **Loops.** The conversation spine is a loop whose body is "await next
  utterance, decide, respond," iterating until disconnect. Deferred in
  `TODO.md`.
- **Parallelism off intermediate results.** Downstream work fires on a
  partial utterance while the source is still producing more. The
  conversation loop and the artefact-building run concurrently.
- **Streaming / long-lived emitting nodes.** A node that stays alive and
  emits a *sequence* of pulses over its lifetime (transcription emitting
  partials), with downstream fan-out per emission. This is distinct from
  the function-callback primitive in `TODO.md`: a callback is
  once-and-back (invoke a subgraph, await, get one result); this is
  many-and-keep-going (emit N times, keep running, each emission fans
  out). `TODO.md` does not yet name this primitive. It is closer to a
  generator / stream / process-with-output-channel than to call-return,
  which suggests it may be more natural for a pulse-based graph than
  function-callbacks are: Weft is structurally a process-network, not a
  call-graph, and a streaming node is the process-network's native
  shape. Open question whether it is a distinct primitive or falls out
  of loops plus parallel emission.
- **Low latency.** The turn loop must close within the human
  conversational window. This constrains how the runtime drives a
  loop body that does not suspend (no journal round-trip per turn).

**Interim shape (what ships before the primitives land).** The entire
call is abstracted behind one configurable node and its infra sidecar.
The sidecar owns the whole live call end to end: transport, STT, the
turn-taking loop, the LLM turns, TTS, and a crash fallback (a
prerecorded message if the session fails). The conversational behaviour
(prompts, assistant config) is configured *on the node*, executed *in
the sidecar*. Weft sees only discrete events the sidecar emits over an
SSE stream (call completed, and any mid-call escalation), each firing a
trigger that begins a durable in-graph workflow. This mirrors the
WhatsApp bridge: a long-lived stateful session in the sidecar, discrete
events into Weft.

The interim shape is deliberate sequencing, not a patch: loops and
function-callbacks come first on the language roadmap, and building the
in-graph conversation on a loop-substitute now would be a shape we would
have to rip out. The durability boundary is honest where it is drawn:
the live call is not durable (a crash ends the call with a fallback
message, never a mid-sentence resume), and the durable guarantee begins
the moment the call ends and the structured result is journaled.

**Why this entry earns its place.** It exercises three deferred items at
once (loops, parallelism, low-latency loop execution) and surfaces one
primitive not yet in `TODO.md` (streaming emitting nodes). When loops
and callbacks are designed, this is the use case to validate them
against.
