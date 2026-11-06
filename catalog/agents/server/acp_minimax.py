"""MiniMax Code (`mcode acp`) as an ACP agent.

Its quirks, and what the spec does about each:

  - Its ask tool (`ask_user`) arrives as an `elicitation/create` form, one
    field per question plus a `<id>__other` field for a typed answer, and
    the tool returns at once ("... is waiting for the local user. Stop this
    turn until the user replies."), so its turn ends right after the
    answer. An accepted answer does not reach the turn that asked: MiniMax
    either queues it (the next `session/prompt` is then refused with "An
    earlier queued message has priority") or runs it as a turn of its own,
    whose reply never comes over ACP and which refuses every prompt while it
    runs ("Session already has an active Turn"). So the form is declined,
    which leaves nothing queued, and the person's answer is what the agent
    reads next, as a message (`answer_message`) the same weft turn goes on
    with once the agent's prompt returns (`Acp.follow_up`).
  - The permission setting is the `permissionMode` config option, scoped to
    the process, so it is set again on every start.
  - The program's tools reach the model directly (`mcp__weft__<tool>`) from
    a command server; only that was seen working.
  - A connection's login lives in `config.yaml` of the private
    `MINIMAX_DATA_DIR`, next to the settings, so the `files` hook writes it
    from the connection's values, with the data contribution turned off.
"""

import json

import adapter_acp as acp
import events as ev
from adapter_acp import AcpSpec, Setup
from events import Refused

# What a custom provider's `api` takes (`mcode provider add --api-format`).
API_FORMATS = ("anthropic-messages", "openai-completions", "openai-responses", "google-generative-ai")


def ask_user(adapter, turn, rid, params):
    """`ask_user`'s form as weft's question; the form is declined and the
    answer is the message the turn goes on with."""
    question, _ = acp.form_question(adapter.question_id(rid), params, adapter.spec.form)

    def result_of(answer):
        given = ev.answers_for(answer, question)
        adapter.follow_up(answer_message([(one["question"], given[one["id"]]) for one in question["questions"]]))
        return {"action": "decline"}

    adapter.ask(turn, rid, question, result_of)


def answer_message(answered):
    """The person's answers as the message MiniMax reads them in: each
    question, then what was picked or typed."""
    return "\n\n".join(f"Q: {text}\nA: {', '.join(values)}" for text, values in answered)


def config_file(adapter):
    """`config.yaml` (JSON is YAML) of the private data dir: the login, and
    no data contribution; nothing on the machine's own login, whose data
    dir is the person's."""
    if adapter.credentials is None:
        return {}
    config = {"dataContribution": {"enabled": False}}
    if adapter.credentials["way"] == "apiKey":
        config.update({"minimax_api": {"apiKey": adapter.login_value("key")}, "minimaxModelSource": "minimax_api_key"})
    else:
        api_format, model = adapter.login_value("apiFormat"), adapter.login_value("model")
        if api_format not in API_FORMATS:
            raise Refused(f"the connection's apiFormat takes {', '.join(API_FORMATS)}, and got {api_format!r}; reconnect it", 400)
        config.update({
            "custom_provider": {"weft": {
                "name": "weft", "kind": "custom", "enabled": True, "api": api_format,
                "options": {"baseURL": adapter.login_value("baseUrl"), "authMode": "api-key", "apiKey": adapter.login_value("key")},
                "models": {model: {}},
            }},
            "defaultModel": f"custom_provider:weft/{model}",
        })
    return {"config.yaml": json.dumps(config, indent=2) + "\n"}


acp.register(AcpSpec(
    "minimax", ["mcode", "acp"],
    permissions={
        "ask": Setup(config={"permissionMode": "default"}),
        "auto": Setup(config={"permissionMode": "auto"}),
        "plan": Setup(config={"permissionMode": "default"}, mode="plan"),
        "bypass": Setup(config={"permissionMode": "bypassPermissions"}),
    },
    model=acp.config_option("model", description="As MiniMax lists it, e.g. m:minimax:MiniMax-M3:v:thinking"),
    options={
        "appendSystemPrompt": acp.flag("--append-system-prompt", description="Text added after its own system prompt"),
        "systemPrompt": acp.flag("--system-prompt", description="Replaces its own system prompt"),
    },
    initialize={"clientCapabilities": {"elicitation": {"form": {}}}},
    mcp="stdio",
    # The form's message is a header over its questions, each field's title
    # its question, and `<id>__other` the typed answer to `<id>`.
    form=acp.Form(message="header", own_answer=acp.Sibling("__other")),
    requests={"elicitation/create": ask_user},
    files=config_file,
    program_tool=acp.ProgramTool(r"mcp__{server}__(?P<name>.+)"),
))
