"""Devin CLI (Cognition), over ACP: `devin acp`."""

import json

import adapter_acp as acp
from adapter_acp import AcpSpec, Setup

# ----- Devin CLI -------------------------------------------------------------
# Run here for real on 3000.11 with a Windsurf key (a plain reply, a call of
# the program's tool, a permission); the rest is its own documentation
# (`share/devin/docs`) and the option ids in its binary. ACP is its native
# interface: the REPL drives a `devin acp` child.
# Its session modes are `accept-edits` (its "Code" mode: edits in the folder
# run, commands and fetches ask), `ask` (answers without changing code),
# `plan` and `bypass`. It has no mode that asks before an edit over ACP, so
# `ask` is `accept-edits` with an `ask` rule for its `edit` tool in its user
# config. A permission request offers `allow_once`, scoped "always" choices
# (`allow_always_global`, `allow_server_always`, `allow_all_fetches`, ...),
# `switch_accept_edits` / `switch_bypass`, which change the session's mode,
# and `reject_once`; organization rules of the account still apply in every
# mode. The request names the call by its id alone: its title and input are
# the `tool_call`'s before it.
# Its ask tool (`ask_user_question`) is a standard `elicitation/create` form
# once the client says it takes forms.
# It reads `WINDSURF_API_KEY`, else the login `devin auth login` stored. It
# also imports, from its home, Claude Code's `~/.claude/CLAUDE.md` and MCP
# servers, Windsurf's, Copilot's, OpenCode's and Zed's: so it runs only on a
# connection's login, in a private home (`HOME`, and the XDG folders taken
# out so they follow it) whose config holds the permission rules and turns
# off its updating itself. Its many `_cognition.ai/...` notifications are
# ignored, and the `server/discover` it sends an MCP server first is
# answered "method not found", after which it speaks plain MCP.

# Its user config, under the private home.
CONFIG = ".config/devin/config.json"
# Each permission setting's rules in that config.
RULES = {"ask": {"ask": ["edit"]}, "accept_edits": {}, "read_only": {}, "plan": {}, "bypass": {}}
# The variables that would move its sessions or login elsewhere, or set what
# a lever sets: taken out always.
MOVED = ("DEVIN_SESSIONS_DIR", "DEVIN_SESSION_STORE", "WINDSURF_API_SERVER_URL", "DEVIN_API_URL", "DEVIN_REMOTE_SESSION_TOKEN",
         "DEVIN_WORKER_TOKEN", "DEVIN_OUTPOSTS_TOKEN", "DEVIN_PERMISSION_MODE", "DEVIN_MODEL", "DEVIN_REFUSAL_FALLBACK")
# The folders that follow HOME once these are out: taken out only in the
# private home, so the machine's own login keeps the person's folders.
XDG = ("XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "XDG_CACHE_HOME")


def devin_files(adapter) -> dict:
    return {CONFIG: json.dumps({"auto_update": False, "permissions": RULES[adapter.permission]}, indent=2)}


acp.register(AcpSpec(
    "devin", ["devin", "acp"],
    permissions={
        "ask": Setup(mode="accept-edits"),
        "accept_edits": Setup(mode="accept-edits"),
        "read_only": Setup(mode="ask"),
        "plan": Setup(mode="plan"),
        "bypass": Setup(mode="bypass"),
    },
    model=acp.flag("--model", description="A model name Devin knows, fuzzy like `/model` (opus, swe-1-6-fast, ...)"),
    options={
        "refusalFallback": acp.Lever(acp.Lever.LIST, lambda models: Setup(env={"DEVIN_REFUSAL_FALLBACK": ",".join(models)}),
                                     description="Models to switch to, in order, when the provider refuses a request under its usage policy"),
    },
    unset_env=MOVED,
    home_unset=XDG,
    initialize={"clientCapabilities": {"elicitation": {"form": {}}}},
    files=devin_files,
    machine_login="on this machine it reads the person's own config and imports their Claude Code, Windsurf, Copilot, "
                  "OpenCode and Zed settings",
    # It titles a call of the program's tool `Calling <tool> from weft`, and
    # names it `mcp__weft__<tool>` in `_meta` (3000.11, a real turn).
    program_tool=acp.ProgramTool(r"mcp__{server}__(?P<name>.+)", at=("_meta", "cognition.ai/toolName")),
))
