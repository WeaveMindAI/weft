"""Junie CLI (JetBrains) over ACP: `junie --acp=true`.

What the spec handles, each seen on Junie 26.11.3 driven against a local fake
model (the notes beside the adapters say how):

  - Permissions are two session config options, set on every start because
    Junie keeps the last `brave_mode` in its home's settings.json for the next
    session: `brave_mode` (`off` asks before every sensitive action, `brave-auto`
    lets its own safety check pass the commands it thinks safe, `on` asks for
    nothing) and `mode` (`plan` swaps in a planning agent with read-only tools).
    Commands it does not count as sensitive (`echo`) run without asking even
    at `off`; a refused action fails that tool and the turn goes on.
  - Its `ask_user` tool is an `elicitation/create` form in which every question
    with options has a sibling `<question>_custom` text field (marked
    `_meta._askUserQuestionCustomAnswer`) for an answer of the person's own:
    one question that allows another answer, which goes back in the sibling
    field (`Form`'s `Sibling`).
  - The program's tools reach it over HTTP. Before each turn a capability filter
    (one model call) picks which MCP tools the turn gets; a server it left out
    is listed to the model as one its `enable_mcp` tool switches on. No setting
    turns the filter off.
  - A failing model call (a wrong key) ends the turn `cancelled` with no reply,
    and the reason is only in the log file it names on stderr.
  - A key for one provider picks that provider; a LiteLLM proxy has no key of
    its own kind, so its login also names the provider in the home's config.json.
  - `--model` takes any name. One the login does not offer (`junie list-models`
    lists more than a Junie token runs, `junie-lite` among them) opens the
    session on another model and fails every prompt with "No model provider is
    available for the selected model", so the session's reported model is
    checked against the node's (seen on Junie 26.11.3 on a real Junie token).
  - With no login (no connection, and none saved by `junie`'s own browser
    login, which it keeps in the system keyring or `~/.junie`'s
    `secure_credentials.json` and its ACP mode reads at start), `session/new`
    answers "Authentication is required before this operation can be performed."
"""

import json

import adapter_acp as acp
from adapter_acp import AcpSpec, Setup

# What marks a form field as the free-text answer of the question it is named after.
OWN_ANSWER = acp.Sibling("_custom", marker="_askUserQuestionCustomAnswer")

# Every effort a model in `junie list-models` takes; each model takes some of
# them, and Junie refuses one its model does not take, naming those it does.
EFFORTS = ["none", "minimal", "low", "medium", "high", "xhigh", "max"]


def junie_files(adapter) -> dict:
    """A LiteLLM login's provider, which no key of its own picks."""
    if adapter.credentials is not None and adapter.credentials["way"] == "litellm":
        return {"config.json": json.dumps({"provider": "litellm"})}
    return {}


def junie_turn_failure(adapter, reply: str):
    """Why a turn with no reply failed, from the log Junie names on stderr."""
    if reply:
        return None
    path = None
    for line in adapter.stderr:
        if "log file: " in line:
            path = line.split("log file: ", 1)[1].strip()
    if path is None:
        return None
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except OSError:
        return None
    for index in range(len(lines) - 1, -1, -1):
        if "Unexpected response:" in lines[index]:
            status = lines[index].split("Unexpected response:", 1)[1].strip()
            body = lines[index + 1].strip() if index + 1 < len(lines) else ""
            return (f"Junie ended the turn without a reply because its model answered {status} {body[:400]} "
                    f"(its log: {path}); check the connection's key and the model")
    return None


def model_ids(option: dict) -> list:
    """Every value a `model` config option offers, its groups flattened."""
    found = []
    for one in option.get("options") or []:
        found += model_ids(one) if "group" in one else [one.get("value")]
    return found


def bare(value: str) -> str:
    """A model value as `junie list-models` names it (`v1:12:jetbrains-ai:gemini-3.6-flash` -> `gemini-3.6-flash`)."""
    return str(value).rsplit(":", 1)[-1]


def junie_model(adapter):
    """The node's model, refused when Junie runs another one: Junie takes any
    `--model`, and for one its login does not offer it opens the session on
    a model of its own choosing, then fails every prompt with "No model
    provider is available for the selected model"."""
    wanted = adapter.config.get("model")
    if not wanted:
        return None
    option = next((o for o in adapter.opened.get("configOptions") or [] if o.get("id") == "model"), None)
    if option is None:
        return f"Junie's session says nothing of its model, so weft cannot tell whether it runs {wanted!r}"
    if bare(option.get("currentValue")) == wanted:
        return None
    offered = ", ".join(sorted({bare(v) for v in model_ids(option) if v}))
    return (f"Junie does not offer the model {wanted!r} on this login (it would run {bare(option.get('currentValue'))!r} "
            f"in its place, and fail every message); set the node's `model` to one it offers: {offered}")


acp.register(AcpSpec(
    "junie", ["junie", "--acp=true", "--skip-update-check"],
    permissions={
        "ask": Setup(config={"mode": "default", "brave_mode": "off"}),
        "auto": Setup(config={"mode": "default", "brave_mode": "brave-auto"}),
        "plan": Setup(config={"mode": "plan", "brave_mode": "off"}),
        "bypass": Setup(config={"mode": "default", "brave_mode": "on"}),
    },
    model=acp.flag("--model", description="A model the login offers (gemini-3.6-flash, claude-sonnet-5-5, gpt-5.5, ...); one it does not is refused, naming those it does"),
    options={
        "effort": acp.config_option("effort", EFFORTS, description="How much the model reasons; each model takes some of these"),
        "appendSystemPrompt": acp.flag("--system-prompt", description="Instructions added to the agent's guidelines"),
    },
    # Settings weft gives itself, never the person's own environment.
    unset_env=["JUNIE_LLM_PROVIDER", "JUNIE_MODEL", "JUNIE_EFFORT", "JUNIE_SYSTEM_PROMPT"],
    initialize={"clientCapabilities": {"elicitation": {"form": {}}}},
    form=acp.Form(own_answer=OWN_ANSWER),
    files=junie_files,
    turn_failure=junie_turn_failure,
    session_check=junie_model,
    program_tool=acp.ProgramTool(r"{server}/(?P<name>.+)"),
))
