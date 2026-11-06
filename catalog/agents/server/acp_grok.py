"""Grok Build (xAI), over ACP: `grok agent stdio`."""

import adapter_acp as acp
import events as ev
from adapter_acp import AcpSpec, Setup

# ----- Grok Build --------------------------------------------------------------
# Its home is `HOME` itself: `GROK_HOME` moves only `~/.grok`, and Grok reads
# the person's Claude Code setup from the home directory beside it
# (`~/.claude/CLAUDE.md` as a rule, `~/.claude/skills`, and the permission
# rules of `~/.claude/settings.json`, which no `compat` switch turns off). In a
# connection's private home there is nothing of theirs to read.
# Agent options go between `agent` and `stdio`, so none is a flag: the model and
# the effort are ACP config options, and the permission settings are the
# session's `_meta` (`yoloMode`, `autoMode`) or its mode (`plan`). Its
# `session/set_mode` takes any id without complaint, `acceptEdits` included,
# and changes nothing for it, so only `default` and `plan` are used.
# It asks with two extension requests of its own: `_x.ai/ask_user_question`
# (the answer is keyed by each question's text, and every question takes
# free text) and `_x.ai/exit_plan_mode` when it is done planning.
# A refused permission ends its whole turn (`stopReason: cancelled`).
# `authenticate` is always needed: `xai.api_key` reads `XAI_API_KEY` (and
# takes any non-empty key: a bad one fails at the first prompt), and
# `cached_token` uses the `grok login` of this machine (it never answers when
# there is none, so that case ends at the request's timeout).
# The program's tools reach the model through Grok's `search_tool` and
# `use_tool`, never as tools of their own; nothing turns that off.


def grok_ask(adapter, turn, rid, params: dict) -> None:
    """`_x.ai/ask_user_question`: one or more questions, each with options and
    a free answer, answered as `{outcome: accepted, answers: {<question>: ...}}`."""
    asked, texts = [], []
    for index, one in enumerate(params.get("questions") or []):
        options = [{"label": o.get("label", ""), "description": o.get("description") or ""} for o in one.get("options") or []]
        asked.append(ev.asked(str(index), "", one.get("question", ""), options, one.get("multiSelect"), True))
        texts.append(one.get("question", ""))
    question = ev.questions(adapter.question_id(rid), asked)

    def result_of(answer: dict) -> dict:
        given = ev.answers_for(answer, question)
        picked = {}
        for one, text in zip(question["questions"], texts):
            values = given[one["id"]]
            picked[text] = values if one["multiSelect"] else values[0]
        return {"outcome": "accepted", "answers": picked}

    adapter.ask(turn, rid, question, result_of)


def grok_exit_plan(adapter, turn, rid, params: dict) -> None:
    """`_x.ai/exit_plan_mode`: the plan is done; approving it leaves plan mode
    and Grok starts on it, keeping it has Grok plan on."""
    choices = [ev.choice("approved", "Approve the plan and start on it", ev.ALLOW_ONCE),
               ev.choice("cancelled", "Keep planning", ev.REJECT)]
    question = ev.permission(adapter.question_id(rid), "plan", "Leave plan mode and start on the plan",
                             params.get("planContent") or "", params, choices)
    adapter.ask(turn, rid, question, lambda answer: {"outcome": ev.chosen(answer, question)["id"]})


acp.register(AcpSpec(
    "grok", ["grok", "agent", "stdio"],
    permissions={
        "ask": Setup(mode="default"),
        "auto": Setup(mode="default", session={"_meta": {"autoMode": True}}),
        "plan": Setup(mode="plan"),
        "bypass": Setup(mode="default", session={"_meta": {"yoloMode": True}}),
    },
    model=acp.config_option("model", description="A Grok model id, such as grok-4.6 (`grok models` lists them)"),
    options={
        "effort": acp.config_option("reasoning_effort", ["low", "medium", "high", "xhigh"],
                                    description="How hard the model reasons (xhigh only on the models that offer it)"),
        "appendSystemPrompt": acp.Lever(acp.Lever.TEXT, lambda text: Setup(session={"_meta": {"rules": text}}),
                                        description="Rules added to its system prompt"),
        "sandbox": acp.variable("GROK_SANDBOX", ["workspace", "devbox", "read-only", "strict"],
                                description="Run its commands in this sandbox profile (needs bubblewrap on Linux)"),
    },
    env={"DISABLE_TELEMETRY": "1", "DISABLE_ERROR_REPORTING": "1"},
    auth_methods={"apiKey": "xai.api_key", acp.MACHINE: "cached_token"},
    resume="session/resume",
    requests={"_x.ai/ask_user_question": grok_ask, "_x.ai/exit_plan_mode": grok_exit_plan},
    reject_ends_turn=True,
    # Its `use_tool` names the program's tool it calls as the model does.
    program_tool=acp.ProgramTool(r"{server}__(?P<name>.+)", at=("rawInput", "tool_name")),
))
