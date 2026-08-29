# Security Policy

Weft runs user-authored code, talks to LLMs, hits external APIs, and stores credentials.

## Reporting a vulnerability

**Do not open a public GitHub issue for security problems.**

Email **contact@weavemind.ai** with:

- A short description of the issue.
- Steps to reproduce (a minimal weft project, a curl command, a code snippet, whatever shows the problem).
- The impact you think it has.
- Your name or handle if you want credit.

We aim to reply within three working days, so chase us if you have heard nothing after a week. From there we work with you on a fix and agree the disclosure timing together. Once the fix has shipped and people have had a window to update, we write the issue up in the release notes and credit you, unless you would rather stay anonymous.

## What counts as a security issue

- Credential leakage: logs, error messages, API responses, the journal, the
  editor's inspector.
- Authentication or authorization bypasses.
- SQL injection, SSRF, or similar classic web vulnerabilities in the
  dispatcher's API, the broker, or the editor.
- Anything that lets one user see or modify another user's projects, executions, or files.
- Denial of service reachable under ordinary conditions, where nothing rate-limits the attacker.

## What does not count

- A project running destructive code (`rm -rf /` and the like) that it authored. A project's code executes with whatever access the worker has, the same as any program you choose to run on your own machine. Only run projects you trust.
- An LLM returning unsafe content. That is a model and prompt issue rather than a weft vulnerability.
- Bugs that require the attacker to already have admin access to your system.

## Scope

This policy covers the `weft` repository and its official binaries. Third-party nodes, community forks, and external services that weft talks to are out of scope, report those to their respective maintainers.

## Hall of fame

People who have reported valid issues will be listed here once we have any.
