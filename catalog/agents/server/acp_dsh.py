"""DeepSeek Harness (`dsh --profile acp`) as an ACP agent.

It does not ask before each action: it runs every command in a sandbox whose
mode `DSH_PERMISSION_MODE` sets when the process starts (it has no session
modes), and asks only when the model wants a command to run outside it. That
request names nothing but the tool call's id, so the question is built from
the `tool_call` update sent just before it. A conversation keeps the sandbox
mode it started with: `session/resume` ignores the variable, so a resume
under another permission setting is refused rather than run under the old
one (`fixed_permission`). It has no `session/load`; `session/resume`
replays nothing. Its telemetry is turned off.

The program's tools are never an MCP server, whose tools it shows as
`mcp__<server>__<tool>`: weft's plugin (`dsh_weft.mjs`) registers each
under its own name, inserted into the profile by a patch file of weft's
own (`--patch`, on every login, so nothing is installed into a profile),
whose config is the route and the tools. It registers them in each
top-level agent's own scope, where a tool shadows a built-in of its name
and nothing else: the built-in's profile row stays on with every other tool
it brings. `run_code` it keeps for itself (the node's `reservedToolNames`).
Its `tool_call` titles a call by the tool's name.
"""

import json
import os

import adapter_acp as acp
import events as ev
import adapters
import program_tools
from adapter_acp import AcpSpec, Lever, Setup
from events import Refused

# Where the patch is, among the files written before each start (`AcpSpec.files`).
PATCH = "weft-tools/weft.patch.yml"
PLUGIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dsh_weft.mjs")


def dsh_files(adapter) -> dict:
    """The patch that inserts weft's plugin with the program's tools, written
    for this start (the route's address is this server's)."""
    if not adapter.tools:
        return {}
    plugin = {"id": "weft-tools", "name": PLUGIN, "config": json.loads(program_tools.listing(adapters.mcp_url(), adapter.tools))}
    # JSON is YAML.
    return {PATCH: json.dumps([{"insert": [plugin]}], indent=1)}


def dsh_tools(adapter) -> Setup:
    """dsh pointed at the patch `dsh_files` writes."""
    return Setup(args=["--patch", os.path.join(adapter.files_dir(), PATCH)])


def dsh_model(value: str) -> Setup:
    """`<route>/<model>` (`deepseek-official/deepseek-v4-flash`) as the
    `model` config option's value, the JSON pair dsh lists it as."""
    route, _, model = value.partition("/")
    if not route or not model:
        raise Refused(f"DeepSeek Harness's `model` is `<route>/<model>`, like `deepseek-official/deepseek-v4-flash`, and got {value!r}", 400)
    return Setup(config={"model": json.dumps([route, model], separators=(",", ":"))})


def dsh_permission(adapter, rid, params: dict):
    """The request to run a command outside the sandbox, with the command in
    it: the request names the call by its id alone, and the `tool_call` the
    agent showed before it (`with_shown_call`) has its name and arguments."""
    call = params.get("toolCall") or {}
    if call.get("toolCallId") not in adapter.shown:
        raise Refused(f"DeepSeek Harness asked to run tool call {call.get('toolCallId')!r} outside its sandbox without "
                      "announcing it first", 500)
    arguments = call.get("rawInput") or {}
    name = call.get("title") or "tool"
    wanted = arguments.get("sandbox_permissions")
    title = arguments.get("description") or name
    if wanted:
        title = f"{title} (outside the sandbox: {wanted})"
    detail = "\n".join(part for part in (arguments.get("command"), arguments.get("justification")) if part)
    choices = [ev.choice(o["optionId"], o.get("name") or o["optionId"], acp.choice_kind(o.get("kind", "")))
               for o in params.get("options") or []]
    question = ev.permission(adapter.question_id(rid), name, title, detail or json.dumps(arguments), arguments, choices)
    return question, lambda answer: acp.permission_answer(question, answer)


acp.register(AcpSpec(
    "dsh", ["dsh", "--profile", "acp"],
    permissions={
        "ask": Setup(env={"DSH_PERMISSION_MODE": "workspace-write"}),
        "read_only": Setup(env={"DSH_PERMISSION_MODE": "read-only"}),
        "bypass": Setup(env={"DSH_PERMISSION_MODE": "danger-full-access"}),
    },
    model=Lever(Lever.TEXT, dsh_model, description="`<route>/<model>`, as its model list names them"),
    options={
        "effort": acp.config_option("reasoning_effort", ["off", "low", "high", "max"],
                                    description="How much it thinks, on a model that offers it"),
    },
    env={"DSH_TELEMETRY_MODE": "DISABLED"},
    resume="session/resume",
    fixed_permission=True,
    permission_hook=dsh_permission,
    files=dsh_files,
    mcp=dsh_tools,
    program_tool=acp.ProgramTool(r"(?P<name>.+)"),
))
