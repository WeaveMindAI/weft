# Security Policy

If you host a Weft program, someone interacting with it should not gain
unexpected access to your machine, credentials or private data because of
a flaw in Weft. Those are the problems we want to hear about, whether they
come through a public endpoint, a browser page or data the program processes.

## Reporting a vulnerability

**Do not open a public GitHub issue for security problems.**

Email **contact@weavemind.ai** with:

- A short description of the issue.
- Steps to reproduce (a minimal weft project, a curl command, a code snippet, whatever shows the problem).
- The impact you think it has.
- Your name or handle if you want credit.

We aim to reply within three working days, so chase us if you have heard nothing after a week. From there we work with you on a fix and agree the disclosure timing together. Once the fix has shipped and people have had a window to update, we write the issue up in the release notes and credit you, unless you would rather stay anonymous.

## What we want reported

- **Exposing controls that should stay private.** A public program URL
  giving access to project management, execution records or administrative
  actions, beyond what the host chose to expose.
- **Leaking credentials or private data.** Secrets appearing in logs,
  errors or the inspector; a service call receiving the wrong credentials;
  a shared file link revealing files it was not meant to expose.
- **Accepting an action without the required permission.** Forged events,
  broken token checks, or a flaw that lets someone submit an approval they
  were not entitled to give. Include ways a malicious website can act
  through the host's browser or Weft extensions.
- **Turning input into unintended access.** A crafted request, file or
  service response causing Weft to execute code, read or overwrite files,
  or contact private services the sender should not be able to reach.
- **Crossing a boundary Weft is supposed to enforce.** Access escaping a
  file reference, token or project permission, or an execution being able
  to interfere with another through a flaw in Weft.
- **Making a hosted program an easy way to exhaust resources.** Inputs
  that make Weft consume disproportionate CPU, memory, storage or paid
  service calls, or keep doing work after it should have stopped.

These are examples, not a checklist you have to fit. If you think Weft
opened a hole in someone's setup, report it even if you aren't sure which
component is responsible. Tell us what the attacker starts with, what they
can reach, and what access they gain.

## Where the boundary is

Weft runs the code in the projects you choose to execute. It is not a
sandbox for running arbitrary untrusted projects. A node deliberately
reading a file or calling a service with access you gave it is different
from an outside request making Weft grant access you never intended.

The local management API also trusts its callers; it is not an authenticated
public hosting interface. If Weft's public-facing routes or supplied
configuration accidentally expose those controls, that is something we
want reported.

Some URLs and tokens grant access to whoever holds them. Using one you were
given is expected. Being able to obtain someone else's through a Weft leak,
use it beyond its intended scope, or bypass its checks is a security issue.

An LLM giving a bad answer is not, by itself, a Weft vulnerability. But if
a model's output can exploit a flaw in Weft to escape the access or approval
boundaries around it, that belongs here. The same applies when a third-party
node or external service is what triggers the flaw.

## What this policy covers

This policy covers the `weft` repository, its official binaries and browser
and editor extensions, and the deployment configuration shipped here.

For a flaw entirely inside a third-party node, a fork or an external service,
contact its maintainer. If the problem also involves Weft, or you cannot
tell where the boundary failed, send it to us and explain what you found.
