# Security Policy

Weft runs user-authored code, talks to LLMs, hits external APIs, and stores credentials. Security matters, and we take it seriously.

## Reporting a vulnerability

**Do not open a public GitHub issue for security problems.**

Email **contact@weavemind.ai** with:

- A short description of the issue.
- Steps to reproduce (a minimal Weft project, a curl command, a code snippet, whatever shows the problem).
- The impact you think it has.
- Your name or handle if you want credit.

You will get an acknowledgement within 48 hours. We will work with you on a fix and coordinate disclosure. Once the fix is shipped and users have had a reasonable window to update, the report becomes public in the changelog and you get credit unless you prefer to stay anonymous.

## What counts as a security issue

- Credential leakage (logs, error messages, API responses, dashboard leaks).
- Authentication or authorization bypasses.
- SQL injection, SSRF, or similar classic web vulnerabilities in the API or dashboard.
- Anything that lets one user see or modify another user's projects, executions, or files.
- Denial of service that is not rate-limited and does not require unusual conditions.

## What does not count

- A project running destructive code (`rm -rf /` and the like) that it authored. A project's code runs with the same access the worker running it has, the same trust boundary as running that project's own program. It is not there to protect you from your own code.
- An LLM returning unsafe content. That is a model and prompt issue, not a Weft vulnerability.
- Bugs that require the attacker to already have admin access to your system.

## Trust model

A project's code runs with the same access the worker running it has: running a project runs its code, exactly as running its own program does. Only run projects you trust, the same way you would only run a program you trust.

## Scope

This policy covers the `weft` repository and its official binaries. Third-party nodes, community forks, and external services that Weft talks to are out of scope, report those to their respective maintainers.

## Hall of fame

People who have reported valid issues will be listed here once we have any.

---

Thanks for helping keep Weft and its users safe.
