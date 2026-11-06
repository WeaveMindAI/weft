"""weft's plugin for Hermes Agent: the program's tools under their own names.

The agent server copies this file into the conversation's private Hermes
home as `plugins/weft/__init__.py`, beside a `plugin.yaml` and the tool list
(`tools.json`, `program_tools.listing`), and names the folder this server's
`program_tools.py` lives in, which goes on `sys.path`, in `server.path`.
Once every tool is registered it writes their names in `loaded.json`, which
the adapter reads as each turn ends: Hermes only logs a plugin that failed to
load, and goes on without its tools.

Hermes shows an MCP server's tools as `mcp__<server>__<tool>` and skips one
whose name a built-in holds. A plugin's tool keeps the name it registers,
and `override=True` (granted by `plugins.entries.weft.allow_tool_override`)
puts it in the place of the built-in of that name, so a program tool named
`web_search` is the one the model calls.
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(HERE, "server.path"), encoding="utf-8") as _f:
    sys.path.insert(0, _f.read().strip())

import program_tools  # noqa: E402

# The toolset weft's tools are registered under, which `platform_toolsets.acp`
# names: the plugin's own id, its folder's name.
TOOLSET = os.path.basename(HERE)


def handler_for(url: str, name: str):
    def handle(args: dict, **_context) -> str:
        result = program_tools.call(url, name, args)
        text = program_tools.text_of(result)
        # Hermes reads a result with an `error` key as the tool's failure.
        return json.dumps({"error": text}) if result.get("isError") else text
    return handle


def register(ctx) -> None:
    from tools.registry import registry
    from agent.deadline import MAX_SAFE_TIMEOUT_S

    listed = program_tools.load(os.path.join(HERE, "tools.json"))
    for tool in listed["tools"]:
        name = tool["name"]
        schema = {"name": name, "description": tool["description"], "parameters": tool["parameters"]}
        # override=True whether or not a built-in holds the name: with the
        # grant it replaces one, and without one it is an ordinary tool.
        handle = ctx.register_tool(name=name, toolset=TOOLSET, schema=schema, handler=handler_for(listed["url"], name),
                                   description=tool["description"], override=True)
        if handle is None:
            # Hermes logs a refused registration and goes on without the
            # tool: the plugin fails instead, so it is never silently missing.
            raise RuntimeError(f"Hermes did not register the program's tool {name!r}")
        # A call waits on weft for as long as weft takes (a person may answer
        # it hours later); Hermes cuts a tool off after 420 s unless its entry
        # says it takes longer, which also runs it on its own rather than
        # beside other calls.
        registry.get_entry(name).execution_timeout = MAX_SAFE_TIMEOUT_S
    # Read by the adapter as each turn ends: every tool is in.
    with open(os.path.join(HERE, "loaded.json"), "w", encoding="utf-8") as f:
        json.dump([tool["name"] for tool in listed["tools"]], f)
