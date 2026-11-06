"""Cursor CLI (Anysphere), over ACP: `agent acp`."""

import adapter_acp as acp
import events as ev
from adapter_acp import AcpSpec, Setup

# ----- Cursor CLI --------------------------------------------------------------
# Its flags are read only before `acp` (after it they start a chat in the
# terminal instead), so they come first and `acp` last. `--trust` takes the
# folder as trusted without the prompt weft could not answer. Bypass is
# `--force`, which runs every command its allowlist does not deny (a team
# whose admin forbids "run everything" falls back to asking).
# A connection's login runs in a private home: `XDG_CONFIG_HOME` holds its
# `cursor/auth.json`, which Cursor fills with the tokens it trades the API
# key for, and which would otherwise be the person's own (a token stored
# there is used before the key); `CURSOR_CONFIG_DIR` holds its settings, its
# allowlist and its conversations, `CURSOR_DATA_DIR` its per-folder data.
# It asks with two requests of its own, both answered for as long as the
# person takes: `cursor/ask_question` (questions that take one or several of
# their options, by id, and no free text) and `cursor/create_plan` (a plan
# to accept, or to reject with a reason the model reads).
#
# The program's tools are weft's MCP server, handed over in `session/new`.
# `agent acp` builds its tool runner without Cursor's MCP meta tools, so each
# one reaches Cursor's backend as a tool of its own (server `weft`, its plain
# name as the tool name), which names it for the model; a call shows as
# `weft: <tool>` (the `mcpToolCall` title, `<providerIdentifier>: <toolName>`).
# Nothing here takes a built-in's place: the `--exclude-tools` and `-H`
# flags that remove built-ins reach only the chat and `--print` commands, not
# `acp`, so a program tool named like a built-in is refused when the program
# is built and when AskAgent fires (the node's `reservedToolNames`).
#
# ACP and not Cursor's SDK (`@cursor/sdk` 1.3.1), which runs the agent in its
# own process: it can remove built-ins (`disallowedTools`), but its tools of
# the host's go behind `GetMcpTools`/`CallMcpTool` all the same, so none
# takes a built-in's name, and it answers what ACP hands to a person by
# itself: every tool runs without asking (its approval store approves all),
# `AskQuestion` is refused ("Interactive questions are not supported in local
# SDK runs") and every plan is accepted. The `ask` and `plan` permissions and
# Cursor's questions exist only over ACP.


def cursor_ask(adapter, turn, rid, params: dict) -> None:
    """`cursor/ask_question`: each question with its options, answered by
    the ids of the options picked."""
    ids = {}
    asked = []
    for q in params.get("questions") or []:
        ids[q["id"]] = {o["label"]: o["id"] for o in q.get("options") or []}
        asked.append(ev.asked(q["id"], "", q.get("prompt", ""), [{"label": o["label"], "description": ""} for o in q.get("options") or []],
                              q.get("allowMultiple"), False))
    question = ev.questions(adapter.question_id(rid), asked)
    if params.get("title") and asked:
        asked[0]["header"] = params["title"]

    def result_of(answer: dict) -> dict:
        given = ev.answers_for(answer, question)
        # A question with no options takes no answer Cursor can read: it
        # answers by option ids alone.
        return {"outcome": {"outcome": "answered", "answers": [
            {"questionId": qid, "selectedOptionIds": [ids[qid][label] for label in labels] if ids[qid] else []}
            for qid, labels in given.items()]}}

    adapter.ask(turn, rid, question, result_of)


ACCEPT, REJECT = "accept", "reject"


def cursor_plan(adapter, turn, rid, params: dict) -> None:
    """`cursor/create_plan`: the plan to accept or reject, the person's note
    on a refusal handed to the model as the reason."""
    todos = "\n".join(f"- {t.get('content', '')}" for t in params.get("todos") or [])
    detail = "\n\n".join(part for part in (params.get("overview"), params.get("plan"), todos) if part)
    question = ev.permission(adapter.question_id(rid), "plan", params.get("name") or "Cursor proposes a plan", detail, params,
                             [ev.choice(ACCEPT, "Accept the plan", ev.ALLOW_ONCE), ev.choice(REJECT, "Reject it", ev.REJECT)])

    def result_of(answer: dict) -> dict:
        if ev.chosen(answer, question)["id"] == ACCEPT:
            return {"outcome": {"outcome": "accepted"}}
        return {"outcome": {"outcome": "rejected", "reason": answer.get("note") or "The person rejected the plan."}}

    adapter.ask(turn, rid, question, result_of)


# What Cursor's ACP layer says, as the turn's last message, when its backend
# refuses the turn for something the account must do (the `action` of the
# error, 6136.index.js), before it ends the prompt as if it went well.
# Seen for real: an account whose plan does not cover the turn.
ACTION_REFUSALS = {
    "Please sign in to continue": "sign in with `agent login` on this machine, or give the node a CursorCliAccess",
    "Upgrade your plan to continue": "its plan does not cover this turn: upgrade it, or pick a `model` the plan includes",
    "Add a payment method to continue": "add a payment method to the Cursor account",
    "Check your settings to continue": "check the Cursor account's settings",
}


def cursor_turn_failure(adapter, reply: str):
    """A turn Cursor ended over an account action, which it says as a
    message (each one after a blank line) and ends as if it went well."""
    for said, what in ACTION_REFUSALS.items():
        if ("\n\n" + reply).rstrip().endswith("\n\n" + said):
            return f"Cursor refused the turn: \"{said}\" ({what}), then send the message again"
    return None


acp.register(AcpSpec(
    "cursor", ["agent", "--trust"],
    subcommand=["acp"],
    permissions={
        "ask": Setup(mode="agent"),
        "plan": Setup(mode="plan"),
        "bypass": Setup(args=["--force"], mode="agent"),
    },
    model=acp.flag("--model", description="A model id Cursor offers your account, such as gpt-5 or sonnet-4-thinking "
                                          "(`agent models` lists them)"),
    options={
        "sandbox": acp.flag("--sandbox", ["enabled", "disabled"],
                            description="Run its commands in Cursor's sandbox (needs unprivileged user namespaces on Linux)"),
        "addDirs": acp.flags("--add-dir", description="More folders it may read and change"),
    },
    unset_env=("CURSOR_AUTH_TOKEN",),
    home_env=("CURSOR_CONFIG_DIR", "CURSOR_DATA_DIR"),
    requests={"cursor/ask_question": cursor_ask, "cursor/create_plan": cursor_plan},
    turn_failure=cursor_turn_failure,
    program_tool=acp.ProgramTool(r"{server}: (?P<name>.+)"),
))
