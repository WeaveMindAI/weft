#!/usr/bin/env python3
"""PermissionRequest hook: a specialist never waits on a permission prompt.

A specialist runs in the background while Tangle and the user are busy
elsewhere, so a prompt it raises waits in the main session until somebody
happens to look, and nothing times out. One such prompt once held a
specialist for a whole night while Tangle reported that nothing was running.

So a permission request coming from a subagent is refused at once, with a
message telling it to find another way or report BLOCKED with the command.
Claude Code puts `agent_id` on the hook input only when the request comes
from inside a subagent; the main session's own requests carry none and pass
through untouched, so the user still answers those as before.

Silence means "not mine to decide": exit 0 with no output and Claude Code
goes on to its usual prompt. A malformed input is said on stderr with a
non-zero exit, which Claude Code shows as a hook error and then prompts as
usual: a broken hook never refuses something it could not read.
"""

import json
import sys

REFUSAL = "never wait on a permission prompt: do it another way, or report BLOCKED with the command"


def main() -> int:
    try:
        request = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        print(f"refuse_specialist_prompts: the hook input is not JSON: {error}", file=sys.stderr)
        return 1
    if not isinstance(request, dict):
        print("refuse_specialist_prompts: the hook input is not a JSON object", file=sys.stderr)
        return 1

    if not request.get("agent_id"):
        return 0

    json.dump(
        {
            "hookSpecificOutput": {
                "hookEventName": "PermissionRequest",
                "decision": {"behavior": "deny", "message": REFUSAL},
            }
        },
        sys.stdout,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
