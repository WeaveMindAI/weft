"""Qwen Code over ACP (`qwen --acp`).

Its quirks, and what the spec does about each:

  - A new session starts in mode `auto`, where a model classifier approves
    what it judges safe; every permission setting names its mode, so `ask`
    sets `default`, where every edit and command comes to weft.
  - Its `ask_user_question` tool arrives as a `session/request_permission`
    whose tool call is marked `_meta.qwenInteractionKind: "user_question"`.
    The answer selects the submit option and carries an `answers` map next to
    the outcome, keyed by the question's index as a string ("0", "1", ...);
    without it the model hears "No valid answers were provided".
  - A refused permission fails the tool and ends the whole turn.
  - `craft/drainMidTurnQueue`, a request it sends after a tool result, is
    answered "method not found" (the turn goes on), and the empty message
    chunks that carry only usage marks are skipped: both the common handling.
  - With a connection's login it runs in a private home (`QWEN_HOME`, the
    whole `~/.qwen`), whose settings choose the login's provider and turn off
    the background extras that cost model calls on every turn: the managed
    memory and its consolidation, and the follow-up suggestions.
  - The program's tools are never an MCP server, whose tools it shows as
    `mcp__<server>__<tool>`: they are tools it discovers by running a
    command (`tools.discoveryCommand`) and calls by running another
    (`tools.callCommand`), both `program_tools.py`, which keep their plain
    names and take the place of a built-in of the same name, any of them
    (each tried with a real turn on 0.26.6). Neither the built-in is turned
    off nor its name denied: either would take the program's tool away too.
    Those settings go in a system settings file of weft's own
    (`QWEN_CODE_SYSTEM_SETTINGS_PATH`, read on every login, above every
    other settings file), which carries the machine's own system settings
    with them. Qwen runs the discovery only while MCP servers start before
    the session (`QWEN_CODE_LEGACY_MCP_BLOCKING=1`): otherwise it starts
    them in the background and never runs it. A call's `tool_call` names
    the tool in `_meta.toolName`.
"""

import json
import os
import shlex
import sys

import adapter_acp as acp
import adapters
import program_tools
from adapter_acp import AcpSpec, Setup
import events as ev
from events import Refused

# The provider each login way selects (`security.auth.selectedType`).
QWEN_AUTH = {"apiKey": "openai", "anthropicKey": "anthropic"}


# The folder, among the files written before each start (`AcpSpec.files`),
# that holds the program's tools.
TOOLS_DIR = "weft-tools"
# Where Qwen reads the system settings when nothing names another file.
SYSTEM_SETTINGS = {"darwin": "/Library/Application Support/QwenCode/settings.json"}
SYSTEM_SETTINGS_ELSEWHERE = "/etc/qwen-code/settings.json"
PROGRAM_TOOLS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "program_tools.py")


def qwen_files(adapter) -> dict:
    """A connection's login settings (`settings.json` of the private home),
    and the program's tools as Qwen's discovered tools (see the module's
    doc): the list, the call command and the system settings naming both,
    written for this start of the agent (the route's address is this server's)."""
    files = {}
    if adapter.credentials is not None:
        files["settings.json"] = json.dumps({
            "security": {"auth": {"selectedType": QWEN_AUTH[adapter.credentials["way"]]}},
            "memory": {"enableManagedAutoMemory": False, "enableManagedAutoDream": False},
            "ui": {"enableFollowupSuggestions": False},
            "tools": {"toolSearch": {"enabled": False}},
        }, indent=2)
    if adapter.tools:
        folder = os.path.join(adapter.files_dir(), TOOLS_DIR)
        listed, call = os.path.join(folder, "tools.json"), os.path.join(folder, "call")
        files[f"{TOOLS_DIR}/tools.json"] = program_tools.listing(adapters.mcp_url(), adapter.tools)
        # Qwen runs the call command as one program with the tool's name as
        # its one argument, never through a shell: a script that runs the real one.
        files[f"{TOOLS_DIR}/call"] = acp.Script(
            f"#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(PROGRAM_TOOLS)} call {shlex.quote(listed)} \"$1\"\n")
        ours = {"tools": {"discoveryCommand": shlex.join([sys.executable, PROGRAM_TOOLS, "declarations", listed]), "callCommand": call}}
        files[f"{TOOLS_DIR}/settings.json"] = json.dumps(acp.deep_merge(system_settings(), ours), indent=2)
    return files


def system_settings() -> dict:
    """The machine's own system settings, which weft's file stands in for."""
    path = os.environ.get("QWEN_CODE_SYSTEM_SETTINGS_PATH") or SYSTEM_SETTINGS.get(sys.platform, SYSTEM_SETTINGS_ELSEWHERE)
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        raise Refused(f"Qwen Code's system settings ({path}) cannot be read ({e}), and weft adds the program's tools "
                      "to them; fix or remove that file") from None


def qwen_tools(adapter) -> Setup:
    """Qwen pointed at the system settings `qwen_files` writes."""
    return Setup(env={"QWEN_CODE_SYSTEM_SETTINGS_PATH": os.path.join(adapter.files_dir(), TOOLS_DIR, "settings.json"),
                      "QWEN_CODE_LEGACY_MCP_BLOCKING": "1"})


def qwen_question(adapter, rid, params):
    """`ask_user_question`: one weft question per question the tool asks."""
    call = params.get("toolCall") or {}
    meta = call.get("_meta") or {}
    if meta.get("qwenInteractionKind") != "user_question":
        return None
    asked = meta.get("qwenQuestions") or []
    submit = [o["optionId"] for o in params.get("options") or [] if o.get("kind") == "allow_once"]
    if not asked or not submit:
        raise Refused(f"{adapter.label} asked a question weft cannot read (no questions or no submit option): "
                      f"{json.dumps(params)[:500]}")
    question = ev.questions(adapter.question_id(rid), [
        ev.asked(str(index), q.get("header", ""), q.get("question", ""),
                 [{"label": o.get("label", ""), "description": o.get("description", "")} for o in q.get("options") or []],
                 q.get("multiSelect"), True)
        for index, q in enumerate(asked)])

    def result_of(answer: dict) -> dict:
        given = ev.answers_for(answer, question)
        return {"outcome": {"outcome": "selected", "optionId": submit[0]},
                "answers": {qid: ", ".join(values) for qid, values in given.items()}}

    return question, result_of


acp.register(AcpSpec(
    "qwen", ["qwen", "--acp"],
    permissions={
        "ask": Setup(mode="default"),
        "accept_edits": Setup(mode="auto-edit"),
        "auto": Setup(mode="auto"),
        "plan": Setup(mode="plan"),
        "bypass": Setup(mode="yolo"),
    },
    model=acp.flag("--model"),
    options={
        "effort": acp.config_option("reasoning_effort", ["low", "medium", "high", "xhigh", "max"],
                                    description="How hard the model thinks (only for a model that reasons)"),
        "appendSystemPrompt": acp.flag("--append-system-prompt", description="Instructions added to its system prompt"),
        "addDirs": acp.joined("--include-directories", ",", description="More folders it may read and change"),
    },
    permission_hook=qwen_question,
    files=qwen_files,
    reject_ends_turn=True,
    mcp=qwen_tools,
    program_tool=acp.ProgramTool(r"(?P<name>.+)", at=("_meta", "toolName")),
))
