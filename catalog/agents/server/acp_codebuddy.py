"""CodeBuddy Code over ACP (`codebuddy --acp`).

Its quirks, and what the spec does about each:

  - Its `AskUserQuestion` tool is offered to the model only when `initialize`
    declares `_meta["codebuddy.ai"].question`. A question then arrives as a
    `session/request_permission` for that tool, and is answered with the
    extension request `_codebuddy.ai/resolveInterruption` (decision `allow`,
    answers keyed `q_<index>`), never through the permission's own reply,
    which stays unanswered: allowing the permission alone leaves the tool
    waiting forever.
  - It defers MCP tools behind a tool search; `CODEBUDDY_DEFER_TOOL_LOADING=0`
    hands the program's tools to the model directly.
  - A permission request names its tool only in `_meta` (its title came in
    the tool call before it), so the question weft asks is titled with it.
  - A refused permission ends its whole turn (`stopReason: cancelled`).
  - `bypassPermissions` still asks before a command it rates dangerous
    (`rm -rf`); `CODEBUDDY_IS_SANDBOX=1` beside it is what lets everything run.
  - No auto-update, telemetry or error reports from a process weft starts.
  - The program's MCP server goes over HTTP with `_meta` naming its tools
    `builtin_tools` under `transparent_proxy`: CodeBuddy then shows each
    under its own name rather than `mcp__weft__<tool>`, and one named like a
    built-in takes the built-in's place in the model's tool list (each of
    its built-ins tried in a real turn on 2.179.3, with nothing turned off:
    the call reached the program's route; `WebSearch` again on 2.180.0
    against its own model). The `tool_call` names the tool in
    `_meta["codebuddy.ai/toolName"]`; its title is the built-in's own once
    the call's arguments are in ("Read File" for a program's `Read`).
    Those keys are not in CodeBuddy's documentation, so a CodeBuddy that
    stops reading them fails the turn, saying so (`codebuddy_names_check`):
    one that shows a program tool as `mcp__weft__<tool>`, or that ends a call
    of a program tool's name the program's route never got (its own
    built-in ran in its place).
"""

import re

import adapters

import adapter_acp as acp
from adapter_acp import AcpSpec, Setup
import events as ev
from events import Refused

ASK_TOOL = "AskUserQuestion"
# The last CodeBuddy Code the plain-name keys were checked on, with a real turn.
NAMES_CHECKED_ON = "2.180.0"
# How it shows an MCP server's tool when it ignores those keys.
PREFIXED = re.compile(rf"mcp__{re.escape(adapters.MCP_SERVER)}__(?P<name>.+)")


def codebuddy_permission(adapter, turn, rid, params: dict) -> None:
    """A permission request, or the ask tool's question riding on one."""
    call = params.get("toolCall") or {}
    tool = (call.get("_meta") or {}).get("codebuddy.ai/toolName")
    # A program tool of that name takes the ask tool's place: its permission
    # is an ordinary one.
    if tool != ASK_TOOL or ASK_TOOL in adapter.program_tools:
        # The request carries no title of its own (the tool call before it
        # did), only the tool's name.
        acp.answer_permission(adapter, turn, rid, {**params, "toolCall": {"title": tool, **call}} if tool else params)
        return
    asked = (call.get("rawInput") or {}).get("questions") or []
    if not asked:
        raise Refused(f"{adapter.label} asked a question with nothing in it: {params}")
    question = ev.questions(adapter.question_id(rid), [
        ev.asked(f"q_{index}", q.get("header", ""), q.get("question", ""),
                 [{"label": o.get("label", ""), "description": o.get("description", "")} for o in q.get("options") or []],
                 q.get("multiSelect"), True)
        for index, q in enumerate(asked)])

    def answered(answer: dict) -> None:
        given = ev.answers_for(answer, question)
        result = adapter.request("_codebuddy.ai/resolveInterruption", {
            "sessionId": adapter.session, "toolCallId": call.get("toolCallId"), "decision": "allow",
            "answers": {qid: ", ".join(values) for qid, values in given.items()}})
        if not result.get("resolved"):
            raise Refused(f"{adapter.label} did not take the answer (it no longer waits on this question): {result}")

    adapter.block(turn, ev.QUESTION, question["id"], {"question": question}, answered, lambda why: adapter.refuse(rid, why))


def codebuddy_tools(adapter) -> Setup:
    """The program's MCP server, its tools under their plain names (see the
    module's doc). CodeBuddy reads that `_meta` only on an `http` or `sse`
    server of `session/new`."""
    server = {"type": "http", "name": adapters.MCP_SERVER, "url": adapters.mcp_url(), "headers": [],
              "_meta": {"transparent_proxy": True, "builtin_tools": sorted(adapter.program_tools.names)}}
    return Setup(session={"mcpServers": [server]})


def codebuddy_names_check(adapter, turn, update: dict):
    """Why the turn cannot go on when CodeBuddy no longer gives the program's
    tools their plain names (see the module's doc), else None."""
    name = (update.get("_meta") or {}).get("codebuddy.ai/toolName") or ""
    prefixed = PREFIXED.fullmatch(name)
    if prefixed is not None and prefixed["name"] in adapter.program_tools.names:
        happened = f"it showed the program's tool `{prefixed['name']}` as `{name}`"
    elif update.get("sessionUpdate") == "tool_call_update" and update.get("status") == "completed" \
            and update.get("toolCallId") in adapter.program_calls:
        tool = adapter.program_tool_of(adapter.shown[update["toolCallId"]])
        with adapter.conv.lock:
            events = list(turn.events)
        program_ids = {i for i in adapter.program_calls if adapter.program_tool_of(adapter.shown[i]) == tool}
        ended = 1 + sum(1 for e in events if e["type"] == ev.TOOL_RESULT and e.get("id") in program_ids)
        routed = sum(1 for e in events if e["type"] == ev.TOOL_CALL and e.get("name") == tool)
        if ended <= routed:
            return None
        happened = f"a call of `{tool}` ended that never reached the program, so its own built-in `{tool}` ran in the program's place"
    else:
        return None
    return (f"{adapter.label} no longer gives the program's tools their plain names: {happened}. weft names them through "
            "keys of CodeBuddy's that it does not document (`transparent_proxy` and `builtin_tools` on the MCP server), "
            f"last checked on CodeBuddy Code {NAMES_CHECKED_ON}, and this CodeBuddy ignores them. Install that version "
            f"(`npm install -g @tencent-ai/codebuddy-code@{NAMES_CHECKED_ON}`), point the node's `program` at its "
            "`codebuddy` and run the turn again. Please also tell weft's maintainers which version dropped them")


acp.register(AcpSpec(
    "codebuddy", ["codebuddy", "--acp"],
    permissions={
        "ask": Setup(mode="default"),
        "accept_edits": Setup(mode="acceptEdits"),
        "auto": Setup(mode="auto"),
        "plan": Setup(mode="plan"),
        "dont_ask": Setup(mode="dontAsk"),
        "bypass": Setup(mode="bypassPermissions", env={"CODEBUDDY_IS_SANDBOX": "1"}),
    },
    model=acp.flag("--model"),
    options={
        "effort": acp.flag("--effort", ["minimal", "low", "medium", "high", "xhigh", "max"],
                           description="How hard the model thinks (only for a model that reasons)"),
        "appendSystemPrompt": acp.flag("--append-system-prompt", description="Instructions added to its system prompt"),
        "addDirs": acp.flags("--add-dir", description="More folders it may read and change"),
        "edition": acp.variable("CODEBUDDY_INTERNET_ENVIRONMENT", ["public", "internal"],
                                description="Which CodeBuddy site the account belongs to: public (codebuddy.ai) or internal (China, copilot.tencent.com)"),
    },
    env={"CODEBUDDY_DEFER_TOOL_LOADING": "0", "DISABLE_AUTOUPDATER": "1", "DISABLE_TELEMETRY": "1", "DISABLE_ERROR_REPORTING": "1"},
    initialize={"clientCapabilities": {"_meta": {"codebuddy.ai": {"question": True}}}},
    requests={"session/request_permission": codebuddy_permission},
    reject_ends_turn=True,
    mcp=codebuddy_tools,
    program_tool=acp.ProgramTool(r"(?P<name>.+)", at=("_meta", "codebuddy.ai/toolName")),
    call_check=codebuddy_names_check,
))
