"""Gemini CLI (Google), over ACP: `gemini --acp`."""

import adapter_acp as acp
from adapter_acp import AcpSpec, Setup


# ----- Gemini CLI ------------------------------------------------------------
# `--skip-trust`: in a folder it was never told to trust, Gemini quietly starts
# none of the MCP servers `session/new` hands it and refuses `yolo` and
# `autoEdit`. Its `ask_user` tool is removed in ACP, so it asks in plain text.
# `authenticate` writes the chosen method into the settings of its home, which
# is the connection's private one.

acp.register(AcpSpec(
    "gemini", ["gemini", "--acp", "--skip-trust"],
    permissions={
        "ask": Setup(mode="default"),
        "accept_edits": Setup(mode="autoEdit"),
        "plan": Setup(mode="plan"),
        "bypass": Setup(mode="yolo"),
    },
    model=acp.flag("--model"),
    options={
        "addDirs": acp.joined("--include-directories", ",", description="More folders it may read and change"),
        "sandbox": acp.variable("GEMINI_SANDBOX", ["docker", "podman", "runsc", "lxc"],
                                description="Run its tools in a container of this kind (needs that runtime on the machine)"),
    },
    auth_methods={"apiKey": "gemini-api-key", "vertexKey": "vertex-ai"},
    program_tool=acp.ProgramTool(r"(?P<name>.+) \({server} MCP Server\)"),
))
