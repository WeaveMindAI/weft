"""Kiro CLI (AWS), over ACP: `kiro-cli acp --agent-engine v3`."""

import adapter_acp as acp
import events as ev
from adapter_acp import AcpSpec, Setup

# ----- Kiro CLI ----------------------------------------------------------------
# `--agent-engine v3`: without it, 3.0 still runs its v2 engine. The v3 engine
# refuses `--model`, `--effort` and the trust flags on `acp`, so the model and
# the effort are ACP config options, and `accept_edits` and `bypass` are the
# `edit-workspace` and `allow-all` policy presets on the session's `_meta`,
# which live in memory only: they are sent again with every load, as every
# setting of `session/new` is (an unknown preset fails the session).
# Its `userInput` tool (the spec workflows' questions) reaches a client that
# says it takes it (`clientCapabilities._meta.kiro.userInput`) as a
# `_kiro/userInput` request, one question with options, answered as text;
# without that it would arrive as a permission request with no free answer.
# A permission request takes the reason of a refusal in its answer's
# `_meta.kiro.rejectionReason`, which the model reads.
# A key in `KIRO_API_KEY` wins over a `kiro-cli login` of this machine, and
# `KIRO_HOME` (the connection's private home) holds its settings and sessions
# in place of `~/.kiro`. The session title it asks a model for on every new
# conversation is turned off: it spends the account's credits.


def kiro_ask(adapter, turn, rid, params: dict) -> None:
    """`_kiro/userInput`: one question, its options by title, and any text
    as the answer."""
    options = []
    for one in params.get("options") or []:
        description = one.get("description") or ""
        subs = [s.get("title", "") for s in one.get("subOptions") or []]
        if subs:
            label = one.get("subOptionsLabel") or "Then"
            description = f"{description} ({label}: {', '.join(subs)})" if description else f"{label}: {', '.join(subs)}"
        options.append({"label": one.get("title", ""), "description": description})
    question = ev.questions(adapter.question_id(rid), [ev.asked("answer", "", params.get("question", ""), options, False, True)])

    def result_of(answer: dict) -> dict:
        return {"action": "answered", "answer": ev.answers_for(answer, question)["answer"][0]}

    adapter.ask(turn, rid, question, result_of)


def kiro_permission(adapter, rid, params: dict):
    """A permission request as the common reading makes it, the person's note
    on a refusal handed to the model as the reason."""
    question = acp.permission_question(adapter.question_id(rid), params)

    def result_of(answer: dict) -> dict:
        result = acp.permission_answer(question, answer)
        if ev.chosen(answer, question)["kind"] == ev.REJECT and answer.get("note"):
            result["_meta"] = {"kiro": {"rejectionReason": answer["note"]}}
        return result

    return question, result_of


acp.register(AcpSpec(
    "kiro", ["kiro-cli", "acp", "--agent-engine", "v3"],
    permissions={
        "ask": Setup(mode="vibe"),
        "accept_edits": Setup(mode="vibe", session={"_meta": {"kiro": {"policyPreset": ["edit-workspace"]}}}),
        "plan": Setup(mode="plan"),
        "bypass": Setup(mode="vibe", session={"_meta": {"kiro": {"policyPreset": ["allow-all"]}}}),
    },
    model=acp.config_option("model", description="A model id Kiro offers your account, such as claude-sonnet-4.5 "
                                                  "(https://kiro.dev/docs/models/)"),
    options={
        "effort": acp.config_option("effortLevel", description="How hard the model reasons, one of the levels the "
                                                                "chosen model offers (ignored by a model with none)"),
        "addDirs": acp.Lever(acp.Lever.LIST, lambda dirs: Setup(session={"additionalDirectories": dirs}),
                             description="More folders it may read and change"),
    },
    env={"KIRO_DISABLE_SESSION_TITLE_LLM": "true"},
    initialize={"clientCapabilities": {"_meta": {"kiro": {"userInput": True}}}},
    requests={"_kiro/userInput": kiro_ask},
    permission_hook=kiro_permission,
    # It titles a call of the program's tool `@weft/<tool>` (3.0.0, a real
    # turn), while the model loads it as `weft::<tool>` through its
    # `tool_load` / `tool_call` pair, and a built-in by its own title.
    program_tool=acp.ProgramTool(r"@{server}/(?P<name>.+)"),
))
