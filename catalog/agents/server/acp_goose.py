"""goose, over ACP (`goose acp`).

goose is a harness over many model providers: it has no login of its own,
only the provider its config names (`GOOSE_PROVIDER`, `GOOSE_MODEL`) and
that provider's key. Each login way is one provider: the key reaches goose
as the provider's own variable, and the provider is written into the
`config.yaml` of the connection's private home, with the keyring off so the
key is read from the environment.

Its home is `HOME` itself: goose finds its config, sessions and logs under
`HOME` (XDG), and it reads skills from `~/.claude/skills` and
`~/.config/agents/skills` with no setting to turn that off (the skills step
is always in its loop), so only a home of its own keeps the person's Claude
skills out of it. A variable any config key can be read from wins over the
config file, so the ones weft sets are taken out first.

The permission setting is its session mode. A refused tool fails that tool
and the model carries on. It has no ask tool: it asks in plain text.
"""

import adapter_acp as acp
from adapter_acp import AcpSpec, Lever, Setup

# The provider each login way's key is for (`goose`'s provider ids).
GOOSE_PROVIDER = {"anthropicKey": "anthropic", "openaiKey": "openai", "googleKey": "google",
                  "openrouterKey": "openrouter", "openaiCompatible": "openai"}


def goose_needs_model(adapter):
    """A connection's key with no model: goose has no default of its own."""
    if adapter.credentials is not None and not adapter.config.get("model"):
        return (f"{adapter.label} needs a model for a connection's key: set `model` to one of "
                f"{GOOSE_PROVIDER[adapter.credentials['way']]}'s model ids")
    return None


def goose_files(adapter):
    if adapter.credentials is None:
        return {}
    return {".config/goose/config.yaml": f"GOOSE_PROVIDER: {GOOSE_PROVIDER[adapter.credentials['way']]}\n"
                                         "GOOSE_DISABLE_KEYRING: true\n"}


acp.register(AcpSpec(
    "goose", ["goose", "acp"],
    permissions={
        "ask": Setup(mode="approve"),
        "smart_approve": Setup(mode="smart_approve"),
        "chat": Setup(mode="chat"),
        "bypass": Setup(mode="auto"),
    },
    model=acp.variable("GOOSE_MODEL"),
    options={
        "effort": acp.variable("GOOSE_THINKING_EFFORT", ["off", "low", "medium", "high", "max"],
                               description="How hard the model thinks, on models that think"),
        "maxTurns": acp.variable("GOOSE_MAX_TURNS", kind=Lever.WHOLE,
                                 description="How many turns it may take without the person's input (1000 when unset)"),
    },
    env={"GOOSE_TELEMETRY_OFF": "true", "GOOSE_DISABLE_SESSION_NAMING": "true"},
    unset_env=["GOOSE_PROVIDER", "GOOSE_MODEL", "GOOSE_MODE", "GOOSE_THINKING_EFFORT", "GOOSE_MAX_TURNS"],
    # What would move its folders out of the private home; on the machine's
    # login they stay the person's own.
    home_unset=["GOOSE_PATH_ROOT", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"],
    settings_refusal=goose_needs_model,
    files=goose_files,
    # Its title is `weft: lookup`; the model's name for it, `weft__lookup`,
    # is in the update's `_meta` beside the extension it went to.
    program_tool=acp.ProgramTool(r"{server}__(?P<name>.+)", at=("_meta", "goose", "toolCall", "toolName")),
))
