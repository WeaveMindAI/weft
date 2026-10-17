#!/usr/bin/env python3
"""PermissionRequest hook: a specialist never waits on an approval prompt.

A specialist runs in its own agent thread while Tangle and the user are busy
elsewhere, so an approval it asks for waits in the overlay until somebody
happens to look, and nothing times out.

So an approval request coming from a subagent is refused at once, with a
message telling it to find another way or report BLOCKED with the command.
Codex puts `agent_id` (the child thread's id) and `agent_type` on a hook's
input only when the hook fires inside a subagent; the root thread's own
requests carry neither and pass through untouched, so the user still answers
those as before.

Silence means "not mine to decide": exit 0 with no output and Codex goes on
to its usual approval prompt. A malformed input is said on stderr with a
non-zero exit, which Codex reports as a hook failure before prompting as
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
