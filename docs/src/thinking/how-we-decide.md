# The three fronts

When a feature lands in weft, it lands on three fronts at once. Miss one and
the feature is half built, usually in a way nobody notices until somebody is
stuck.

**Levers.** What can a person reach, and from where? A setting on a node, a
flag on the CLI, a control in the graph, something the node's Rust asks the
ctx for, a key in `metadata.json`, a word in the language itself. Name the
lever and name where it lives. A knob nobody can turn is not a lever, and a
knob nobody needs is clutter.

**Defaults.** What does everybody get without asking? This is the part a
person should never have to know exists. If the right behaviour only happens
when somebody sets something, either the default is wrong, or the setting has
to be required, and then leaving it out is a validation error that says which
setting is missing.

Two questions decide it. Is it tedious and useful to make somebody fill this in
every time? Is there a number or a choice that is right for almost everybody?
Yes to both and you set it, and the lever is there for whoever wants to change
it. But never invent a default to paper over a setting that genuinely depends
on the case. If there is an honest reason to leave it unset, leaving it unset
has to stay possible, and then the setting is explicit and the validation is
what makes the bad shape impossible. A hidden default that is right half the
time is worse than a refusal that says what is missing.

**Protection.** What stops somebody building the broken version by accident?
The compiler, the parser, and a node's own validation rules know things before
anything runs. If a shape is provably wrong, refuse it at the earliest point
that can see it, and say what to do instead. The obvious ways to hurt yourself
(a loop with no way out, a run left waiting forever on somebody who is gone)
should be impossible to write down in the first place, and where something can
only be caught while it runs, the runtime stops it and the message points at
the shape that works.
