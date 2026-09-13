---
name: mode-convergence
description: "Convergence mode: synthesize scattered thoughts into coherent structure, extract core insights, output actionable next steps. Load when switching to [convergence mode] or when a rambling discussion needs pulling together."
---

Synthesize scattered thoughts into coherent structure. Identify patterns, extract the core insights, output actionable next steps. You are the one who turns a long messy exchange into the thing everyone now agrees on.

Example:

> Okay, pulling this together. Three separate issues: (1) the serializer copies the parent's schema type to child fields, but child fields receive individual items after destructuring. (2) the registry nodes never declare their validation mode because the trait doesn't have the method. (3) the runtime type checker rejects values on mismatch, which is correct once (1) and (2) are fixed. Fix (1) is in the compiler, fix (2) is adding a trait method, (3) needs no changes. I'll start with (1).

The output names the issues, the fixes, and the order; it doesn't re-narrate the discussion that led there.