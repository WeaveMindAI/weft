"""The program's tools as a JavaScript plugin of the agent's own, for an
agent whose plugins name a tool plainly (`search`) where its MCP client
would prefix it (`mcp__weft__search`): Pi, Kilo Code, OpenCode, OpenClaw.

Each of them has one plugin file in this folder (`pi_weft.ts`,
`kilo_weft.mjs`, ...), whose `make` makes the plugin from the tool list
(`program_tools.listing`: the route and the tools), and which forwards
every call to the agent server's MCP route through
`weft_tools.mjs`, where a call is the program's whatever its name. At
every start the adapter writes a loader into a folder the agent reads,
handing that plugin this conversation's route and tools. The tools are
fixed for the life of one agent process (another tool list starts another
process), so the loader is too.
"""

import json
import os
import pathlib

import adapters
import program_tools

HERE = os.path.dirname(os.path.abspath(__file__))


def plugin_file(name: str) -> str:
    """A plugin file of this folder (`kilo_weft.mjs`), by its path."""
    path = os.path.join(HERE, name)
    if not os.path.exists(path):
        raise SystemExit(f"js_plugin: the agent server has no plugin file {name} beside it ({HERE})")
    return path


def loader(plugin: str, tools: list) -> str:
    """The loader of `plugin` (a path `plugin_file` gave) for `tools`: an ES
    module whose default export is the plugin, made for this conversation
    by the plugin file's named export `make`."""
    listed = json.loads(program_tools.listing(adapters.mcp_url(), tools))
    return ("// Written by weft's agent server at every start of the agent: the program's tools,\n"
            "// forwarded to the agent server. Edits are lost at the next start.\n"
            f"import {{ make }} from {json.dumps(uri(plugin))};\n"
            f"export default make({json.dumps(listed, indent=1)});\n")


def write_loader(path: str, plugin: str, tools: list) -> str:
    """Write the loader of `plugin` for `tools` at `path` (a folder of weft's
    own, never the person's), whole, so the agent never reads half of one.
    It holds no secret: the route and the tools. Returns `path`."""
    adapters.write_whole(path, loader(plugin, tools), 0o644)
    return path


def uri(path: str) -> str:
    return pathlib.Path(path).as_uri()
