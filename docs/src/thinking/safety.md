# Our approach to AI safety

Weft is built as a capability tool. The design has a second consequence that
some readers care about, and this page is where it lives.

## Why a language

Today an AI system's actual structure, which model sees what, what a human
gates, which step can spend money or delete something, exists as control flow
spread across a dozen files. No tool can read it, so every guarantee about it
is a promise somebody made in a review.

Weft makes that structure a first-class artifact the compiler reads, so
properties about it become checkable.

## Rigor as a dial

Rigor is a dial here, set per program, at the level that program needs.

A side project connects things fast, iterates raw, leaves the model free, and
nothing gets in the way. A system where the stakes are real turns on the checks
it needs, and gets a machine-checked answer instead of a code review
convention.

## What becomes provable

None of this is shipped. It is what the design is aimed at.

Because the orchestration is data, a compiler flag can turn a policy into a
property of compilation:

- every path into this node passes a validation step,
- this use case requires these audited nodes between the model and the effect,
- every path out of this black-box node goes through a deterministic switch,
- this value must be deterministic.

Take that last one concretely. An email address a program is about to send to,
if it came out of a model, is not deterministic, and a determinism flag refuses
to compile the program. If it came from a node whose output is proven
deterministic, the compiler can demonstrate the property rather than take
anyone's word.

Extend that to a node whose determinism was established by an audit and stamped
as trusted, and a compiler flag becomes the difference between "we believe this
pipeline is compliant" and "this pipeline does not compile unless it is".

## Why this needs a language

All of it depends on the structure being legible to a machine before anything
can be proved about it. A library sits inside a language that cannot see the
shape, so the best it can offer is a convention and a runtime check. The
orchestration has to be the source for a compiler to have anything to work
with.

## What exists today

The compiler checks types, connection completeness, graph shape, and each
node's own declared validation rules. Every execution is journaled, so what a
program actually did is recoverable rather than reconstructed.

The policy flags above are designed and not built. They are the direction the
type system and the validator are being grown toward.

## The longer argument

The reasoning behind this, including why the interesting unit of control is the
system around a model rather than the model itself, is in
[The future of programming](https://weavemind.ai/blog/future-of-programming) and
[Three properties for alignment](https://weavemind.ai/blog/three-properties-for-alignment).
