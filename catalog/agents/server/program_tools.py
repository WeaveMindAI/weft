"""The program's tools, called from inside an agent: what a plugin, an
extension or a script the agent runs uses to hand a call to weft.

Some agents show the tools of an MCP server to their model only under a
prefix (`mcp__weft__search`), and keep their own built-in of the same
name. The same agents let a plugin of their own register a tool under
any name, a built-in's included. So for them the agent server writes a
plugin (or a pair of commands) that registers each of the program's
tools under its plain name and forwards every call here, and from here to
the program's tool route: the agent server's MCP server, which counts
every call arriving there as the program's.

Nothing here imports anything but the standard library: it runs inside
the agent's own Python (Hermes' and nanobot's environments) as well as
this server's.

The tool list a plugin registers is a file the agent server writes at
each start (`listing`): the route's address and every tool's name,
description and parameters, as the program gave them.

As a command, for an agent that runs one per discovery and per call
(Qwen Code's `tools.discoveryCommand` and `tools.callCommand`):

    program_tools.py declarations <list>        the tools, as function declarations
    program_tools.py call <list> <name>         one call, its arguments on stdin:
                                                its output on stdout, or, when
                                                it failed, on stderr with exit 1
"""

import json
import os
import sys
import urllib.error
import urllib.request


class Unanswered(Exception):
    """The route did not answer the call (the agent server is gone), or
    answered it with a protocol error: the call failed, and says why."""


def listing(url: str, tools: list) -> str:
    """The text of the tool list a plugin reads (`load`): the route's `url`
    and the tools. Its caller writes it, with the files it writes beside."""
    return json.dumps({"url": url, "tools": [{"name": t["name"], "description": t.get("description") or "",
                                               "parameters": t.get("parameters") or {"type": "object", "properties": {}}}
                                              for t in tools]}, indent=1)


def load(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def call(url: str, name: str, arguments: dict) -> dict:
    """One call of the program's tool `name`: MCP's `tools/call` result
    (`content`, `isError`). No timeout: weft holds the call until it is
    answered, and a person may take hours to answer it."""
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": name, "arguments": arguments or {}}}).encode("utf-8")
    request = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=None) as answer:
            reply = json.loads(answer.read() or b"{}")
    except (urllib.error.URLError, OSError) as e:
        raise Unanswered(f"weft's agent server did not answer the call of {name!r}: {e}") from None
    if "error" in reply:
        raise Unanswered(f"weft's agent server refused the call of {name!r}: {reply['error'].get('message', reply['error'])}")
    return reply.get("result") or {}


def text_of(result: dict) -> str:
    """The text of a `tools/call` result, every text item joined."""
    return "\n".join(item.get("text", "") for item in result.get("content") or [] if item.get("type") == "text")


def declarations(listed: dict) -> list:
    """The tools as function declarations (`name`, `description`, `parameters`)."""
    return [{"name": t["name"], "description": t["description"], "parameters": t["parameters"]} for t in listed["tools"]]


def main(argv: list) -> int:
    if len(argv) == 2 and argv[0] == "declarations":
        print(json.dumps(declarations(load(argv[1]))))
        return 0
    if len(argv) == 3 and argv[0] == "call":
        listed = load(argv[1])
        arguments = json.loads(sys.stdin.read() or "{}")
        try:
            result = call(listed["url"], argv[2], arguments)
        except Unanswered as e:
            print(str(e), file=sys.stderr)
            return 1
        # The answer's text is the tool's output, as the agent hands it to its
        # model; a failed call is its text on stderr and a nonzero exit,
        # which is how such an agent tells a failure from an output.
        if result.get("isError"):
            print(text_of(result), file=sys.stderr)
            return 1
        sys.stdout.write(text_of(result))
        return 0
    print(f"usage: {os.path.basename(__file__)} declarations <list> | call <list> <name>", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
