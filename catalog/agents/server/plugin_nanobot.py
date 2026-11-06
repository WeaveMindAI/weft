"""weft's tool plugin for nanobot: the program's tools under their own names.

nanobot shows an MCP server's tools as `mcp_<server>_<tool>`. A tool from
an entry point of group `nanobot.tools` keeps its own name, so the adapter
makes the program's tools one: at every start it writes, in a folder of
the conversation's nanobot instance that goes on the gateway's
`PYTHONPATH`,

  - this file, as `weft_nanobot.py`, which reads `tools.json` beside it
    (`program_tools.listing`) and makes one tool class per program tool,
    `tool_0`, `tool_1`, ...;
  - `weft_nanobot_tools-1.dist-info/entry_points.txt`, naming each class
    in that group, which is how nanobot finds them (an installed package
    is only a folder on `sys.path` holding such a file).

A plugin tool with a built-in's name is skipped while that built-in is
registered, so the adapter turns the built-in off for it (only one whose
group holds nothing else; the build refuses the other names).
"""

import asyncio
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(HERE, "server.path"), encoding="utf-8") as _f:
    sys.path.insert(0, _f.read().strip())

import program_tools  # noqa: E402
from nanobot.agent.tools.base import Tool, ToolResult  # noqa: E402


def tool_class(url: str, tool: dict) -> type:
    class ProgramTool(Tool):
        # The main agent's only: a subagent nanobot starts is the model's
        # own helper, and a call of the program's tool belongs to weft's turn.
        _scopes = {"core"}

        @property
        def name(self) -> str:
            return tool["name"]

        @property
        def description(self) -> str:
            return tool["description"]

        @property
        def parameters(self) -> dict:
            return tool["parameters"]

        async def execute(self, **kwargs) -> str:
            # The call is held until weft answers it, which may take hours:
            # off the event loop, so the gateway goes on serving meanwhile.
            result = await asyncio.to_thread(program_tools.call, url, tool["name"], kwargs)
            text = program_tools.text_of(result)
            return ToolResult.error(text) if result.get("isError") else text

    ProgramTool.__name__ = ProgramTool.__qualname__ = f"WeftTool_{tool['name']}"
    return ProgramTool


_listed = program_tools.load(os.path.join(HERE, "tools.json"))
for _index, _tool in enumerate(_listed["tools"]):
    globals()[f"tool_{_index}"] = tool_class(_listed["url"], _tool)

