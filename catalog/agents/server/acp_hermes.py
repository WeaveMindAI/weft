"""Hermes Agent (Nous Research), over ACP: `hermes acp`."""

import json
import os
import shutil
import signal
import subprocess
import time

import adapter_acp as acp
import adapters
import program_tools
from adapter_acp import AcpSpec, Setup
from events import Refused

# ----- Hermes Agent ------------------------------------------------------------
# `hermes acp` runs no agent: it starts (or joins) a background gateway daemon
# for its `HERMES_HOME` and relays to it, and the daemon outlives it. Every
# conversation has a home of its own, so the daemon there is this
# conversation's alone, and it is stopped by the pid it writes in
# `gateway.pid` whenever the `hermes acp` in front of it is (once that pid is
# checked to still be the gateway).
# On the machine's own Hermes, its `config.yaml` decides whether it borrows
# Claude Code's and Codex's logins (`auth.adopt_external_logins`, on unless
# turned off), and the machine's daemon is shared with whatever else runs
# Hermes there, so it runs only on a connection's login, in a home whose
# `config.yaml` this file writes:
#   - the login's provider and the node's model (Hermes refuses a session
#     with no model, as `storage_unavailable`);
#   - borrowed logins off;
#   - its `clarify` tool back in the ACP tools (left out by default), which
#     asks as an `elicitation/create` form once the client says it takes one;
#   - the MCP tools as tools of their own, rather than behind its tool search;
#   - dangerous commands asked about every time (`manual`) or never (`off`),
#     and neither a permission nor a question given up on while weft waits
#     (by default a permission is denied after 300 s, a question after 3600 s);
#   - the background model calls that cost a request on every turn off (the
#     title upgrade, the post-turn self review), and its update check;
#   - its commands left off PATH (`cli.expose_on_path`): a launch under a
#     home it was not installed in otherwise writes `hermes` shims into
#     `~/.local/bin`, pointing the person's own `hermes` at weft's install;
#   - with the program's tools, weft's plugin (`plugin_hermes.py`) in
#     `plugins/weft/`, enabled, allowed to take a built-in's name, and its
#     toolset in the ACP tools.
# The program's tools are never handed over as an MCP server: Hermes would
# show them as `mcp__<server>__<tool>` and skip one whose name a built-in
# holds. The plugin registers each under its own name, in the place of the
# built-in of that name, except the built-ins Hermes handles by name before
# any tool runs (the node's `reservedToolNames`): its agent loop's own
# (`todo_list`, `memory`, `session_search`, `delegate_task`), `clarify` and
# `manage_connections`, and `write_file` and `patch`, whose call its ACP
# side turns into an edit to approve. Its `tool_call` titles a call
# `<name>` or `<name>: <a preview of the arguments>`.
# Only a command on its dangerous list asks: an ordinary one runs. Edits are
# the ACP session mode. A permission is answered by its option id, and its
# `allow_session` and `allow_always` are both kind `allow_always`; the
# second writes the command into `config.yaml`, which is written again each
# time the agent starts, so it lasts as long as the agent process.

# The longest Hermes waits on a permission, in seconds (it clamps it to a year).
PERMISSION_TIMEOUT_SECS = 31536000
# How long a stopping daemon gets to end on its own before it is killed.
DAEMON_GRACE_SECS = 5
# What Hermes writes as `kind` in the `gateway.pid` of a gateway.
GATEWAY_KIND = "hermes-gateway"

# Where weft's plugin lives in the private home, its id there, and the
# toolset its tools are registered under (the plugin's folder's name).
PLUGIN = "weft"
PLUGIN_DIR = f"plugins/{PLUGIN}"
PLUGIN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugin_hermes.py")
# What the plugin writes once it has registered every tool: the names, which
# the adapter checks once the session is made (Hermes logs a plugin that
# failed to load and goes on without it).
PLUGIN_LOADED = f"{PLUGIN_DIR}/loaded.json"

# The provider each login way selects (`model.provider`).
PROVIDERS = {"openRouter": "openrouter", "anthropicKey": "anthropic", "openaiKey": "openai-api",
             "geminiKey": "gemini", "openaiCompatible": "custom"}


def hermes_config(adapter) -> dict:
    model = adapter.config.get("model")
    if not model:
        raise Refused(f"{adapter.label} needs `model`: the model id its provider names it by, such as "
                      "anthropic/claude-sonnet-4.5 on OpenRouter", 400)
    way = adapter.credentials["way"]
    chosen = {"provider": PROVIDERS[way], "default": model}
    if way == "openaiCompatible":
        chosen.update(base_url=adapter.login_value("baseUrl"), key_env="OPENAI_API_KEY")
    config = {
        "model": chosen,
        "auth": {"adopt_external_logins": False},
        "approvals": {"mode": "off" if adapter.permission == "bypass" else "manual", "timeout": PERMISSION_TIMEOUT_SECS},
        "agent": {"clarify_timeout": 0},
        "tools": {"tool_search": {"enabled": "off"}},
        "platform_toolsets": {"acp": ["hermes-acp", "clarify"]},
        "auxiliary": {"title_generation": {"model_upgrade_enabled": False}, "background_review": {"enabled": False}},
        "updates": {"check": False},
        "cli": {"expose_on_path": False},
    }
    files = {}
    if adapter.tools:
        config["platform_toolsets"]["acp"].append(PLUGIN)
        config["plugins"] = {"enabled": [PLUGIN], "entries": {PLUGIN: {"allow_tool_override": True}}}
        with open(PLUGIN_FILE, encoding="utf-8") as f:
            files[f"{PLUGIN_DIR}/__init__.py"] = f.read()
        files[f"{PLUGIN_DIR}/plugin.yaml"] = json.dumps({"name": PLUGIN, "version": "1",
                                                         "description": "The weft program's tools",
                                                         "provides_tools": sorted(adapter.program_tools.names)})
        files[f"{PLUGIN_DIR}/server.path"] = os.path.dirname(PLUGIN_FILE)
        files[f"{PLUGIN_DIR}/tools.json"] = program_tools.listing(adapters.mcp_url(), adapter.tools)
        # Emptied here, written by the plugin once it has loaded.
        files[PLUGIN_LOADED] = ""
    # JSON is YAML.
    files["config.yaml"] = json.dumps(config, indent=2)
    return files


# Hermes' uv download cache, in every home it runs in: `<HERMES_HOME>/cache/uv`
# (`pm/packages.py`, `uv_cache_path`), whatever `UV_CACHE_DIR` says (its
# `pm/environment.py` sets that variable for uv itself). A home other than
# its install's builds a Python environment of its own on its first start
# (`_prepare_borrowed_launch`), some 430 MB of files of which about 300 MB
# are hard links into that cache (uv's default way of filling an environment,
# which is why `UV_LINK_MODE` is taken out): with the cache shared, a
# conversation home holds about 130 MB of its own; with one of its own, 440.
UV_CACHE = os.path.join("cache", "uv")
# The folder, beside `bin/`, of weft's own Hermes install (agents.json's
# `hermes` recipe): Hermes' own home there, which holds the checkout.
INSTALL_HOME = "hermes"


def install_uv_cache(adapter):
    """The uv cache of weft's own Hermes install, the one the adapter runs
    (its `bin/hermes`), or None for a Hermes of the person's own, whose
    home weft never writes into."""
    found = shutil.which(adapter.program())
    if found is None:
        return None
    home = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(found))), INSTALL_HOME)
    if not os.path.isdir(os.path.join(home, "hermes-agent")):
        return None
    return os.path.join(home, UV_CACHE)


def share_uv_cache(adapter) -> None:
    """Point the conversation home's uv cache at weft's install's (a link),
    so every conversation's first start reuses the downloads the install
    already made rather than fetching its own: uv shares one cache between
    processes safely, and Hermes keeps one cache for all the profiles of a
    home on purpose. A home made before holds a cache of its own, which
    goes (its environment's hard links into it outlive it). A Hermes of the
    person's own keeps a cache in each conversation home: removing it would
    free only the files no environment links (about 10 MB)."""
    shared = install_uv_cache(adapter)
    if shared is None:
        return
    link = os.path.join(adapter.home, UV_CACHE)
    if os.path.islink(link):
        if os.readlink(link) == shared:
            return
        os.remove(link)
    elif os.path.isdir(link):
        shutil.rmtree(link)
    os.makedirs(shared, exist_ok=True)
    os.makedirs(os.path.dirname(link), exist_ok=True)
    os.symlink(shared, link)


def hermes_files(adapter) -> dict:
    """The conversation home as Hermes needs it before it starts: its uv
    cache shared where it can be (`share_uv_cache`), and its files."""
    share_uv_cache(adapter)
    return hermes_config(adapter)


def plugin_failure(adapter):
    """Why the session cannot go on, when Hermes did not load weft's plugin
    and so would run without the program's tools. Hermes loads its plugins
    as it makes the session, so by `session/new`'s answer (or the load's)
    the plugin has said so or never will."""
    if not adapter.tools:
        return None
    try:
        with open(os.path.join(adapter.home, PLUGIN_LOADED), encoding="utf-8") as f:
            loaded = f.read()
    except FileNotFoundError:
        loaded = ""
    if loaded and sorted(json.loads(loaded)) == sorted(adapter.program_tools.names):
        return None
    said = "\n".join(line for line in adapter.stderr if f"'{PLUGIN}'" in line) or adapter.stderr_tail()
    return f"Hermes did not load weft's plugin, which holds the program's tools, so it would run without them. It says: {said}"


def stop_daemon(adapter) -> None:
    """The gateway daemon `hermes acp` started in this conversation's home,
    with what it started (a session of its own: an update check, a tool's
    command), then its `gateway.pid`. A pid the file names that is no
    longer that gateway (it was killed, and the number went to another
    process) is never signalled. Idempotent: a stopped gateway leaves no file."""
    path = os.path.join(adapter.home, "gateway.pid")
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except FileNotFoundError:
        return
    pid = gateway_pid(path, text)
    if pid is not None:
        stop_gateway(pid)
    try:
        os.remove(path)
    except FileNotFoundError:
        # The gateway takes its own file away as it stops.
        pass


def gateway_pid(path: str, text: str):
    """The pid `gateway.pid` names, while that process is still the gateway
    that wrote it, else None; refused when that cannot be told. Hermes
    writes `pid`, `kind`, `argv`, `start_time` and `hermes_home`; its
    `start_time` is field 22 of `/proc/<pid>/stat` where there is a `/proc`,
    else the process's birth in hundredths of a second since the epoch (from
    psutil, None without it). A pid is matched to its record by the start
    time, read here from `/proc`, else from `ps`: a number another process
    took since is never signalled."""
    try:
        record = json.loads(text)
    except json.JSONDecodeError as e:
        raise Refused(f"Hermes' {path} is not the JSON Hermes writes ({e}), so its gateway may still run; stop it and "
                      "delete the file") from None
    if not isinstance(record, dict) or record.get("kind") != GATEWAY_KIND or not isinstance(record.get("pid"), int):
        raise Refused(f"Hermes' {path} names no gateway (`kind` {GATEWAY_KIND!r} and a `pid`): {text[:200]!r}; stop the "
                      "gateway it belongs to, if any, and delete the file")
    pid, recorded = record["pid"], record.get("start_time")
    if not isinstance(recorded, int):
        # Without it, a pid another process took since looks like the gateway.
        raise Refused(f"Hermes' {path} records no start time for gateway {pid}, so weft cannot tell it from a process "
                      "that took its number since; stop that gateway yourself, if it runs, and delete the file")
    if os.path.isdir(PROC):
        found = proc_identity(path, pid)
        same = found is not None and found[0] == recorded
    else:
        found = ps_identity(path, pid)
        same = found is not None and abs(found[0] - recorded / 100) <= PS_START_SLACK_SECS
    if not same or "gateway" not in found[1]:
        return None
    return pid


# Where a process's start time and command line are read, when there is one.
PROC = "/proc"
# How far `ps`'s start of a process may be from the one Hermes recorded:
# `ps` gives whole seconds, and Hermes' own check of the same reading allows
# 2 s of drift (`START_TIME_DRIFT_TOLERANCE`, a clock step on macOS).
PS_START_SLACK_SECS = 3


def proc_identity(path: str, pid: int):
    """`pid`'s start time (field 22 of its `stat`) and the words of its
    command line, from `/proc`; None when it is gone."""
    try:
        with open(f"{PROC}/{pid}/stat", "rb") as f:
            stat = f.read()
        with open(f"{PROC}/{pid}/cmdline", "rb") as f:
            words = [word.decode("utf-8", "replace") for word in f.read().split(b"\0")]
    except FileNotFoundError:
        return None
    except PermissionError as e:
        raise Refused(f"cannot read {PROC}/{pid} ({e}) to tell whether it is still the gateway Hermes' {path} names; "
                      "stop that gateway yourself, if it runs, and delete the file") from None
    return int(stat.rsplit(b")", 1)[1].split()[19]), words


def ps_identity(path: str, pid: int):
    """`pid`'s start (seconds since the epoch) and the words of its command
    line, from `ps` (a machine with no `/proc`: macOS); None when it is gone."""
    try:
        done = subprocess.run(["ps", "-ww", "-o", "lstart=,command=", "-p", str(pid)], capture_output=True, text=True,
                              env=dict(os.environ, LC_ALL="C"), check=False)
    except OSError as e:
        raise Refused(f"cannot run `ps` ({e}) to tell whether pid {pid} is still the gateway Hermes' {path} names; "
                      "stop that gateway yourself, if it runs, and delete the file") from None
    words = done.stdout.split()
    if done.returncode != 0 or not words:
        return None
    try:
        # `lstart` is five words: `Wed Nov  4 10:00:01 2026`, in local time.
        started = time.mktime(time.strptime(" ".join(words[:5]), "%a %b %d %H:%M:%S %Y"))
    except ValueError:
        raise Refused(f"`ps` gave a start for pid {pid} weft cannot read ({done.stdout.strip()[:200]!r}), so it cannot "
                      f"tell whether it is still the gateway Hermes' {path} names; stop that gateway yourself, if it "
                      "runs, and delete the file") from None
    return started, words[5:]


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def stop_gateway(pid: int) -> None:
    """SIGTERM, then SIGKILL past the grace: its whole session when it leads one."""
    try:
        group = os.getpgid(pid)
    except ProcessLookupError:
        return
    kill = (lambda sig: os.killpg(pid, sig)) if group == pid else (lambda sig: os.kill(pid, sig))
    try:
        kill(signal.SIGTERM)
        deadline = time.monotonic() + DAEMON_GRACE_SECS
        while time.monotonic() < deadline:
            os.kill(pid, 0)
            time.sleep(0.1)
        adapters.log(f"Hermes' gateway {pid} did not stop within {DAEMON_GRACE_SECS}s of SIGTERM; killing it")
        kill(signal.SIGKILL)
    except ProcessLookupError:
        pass


acp.register(AcpSpec(
    "hermes", ["hermes", "acp"],
    permissions={
        "ask": Setup(mode="default"),
        "accept_edits": Setup(mode="accept_edits"),
        "auto": Setup(mode="dont_ask"),
        "bypass": Setup(mode="dont_ask"),
    },
    model=acp.in_files(description="The model id its provider names it by, such as anthropic/claude-sonnet-4.5 on "
                                   "OpenRouter or claude-sonnet-4-5 with an Anthropic key"),
    options={
        "effort": acp.config_option("reasoning_effort", ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"],
                                    description="How hard the model reasons (a model that does not reason ignores it)"),
    },
    env={"HERMES_KANBAN_DISPATCH_IN_GATEWAY": "0"},
    unset_env=("ANTHROPIC_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN", "GOOGLE_API_KEY", "ANTHROPIC_BASE_URL", "OPENROUTER_BASE_URL",
               "GEMINI_BASE_URL", "HERMES_YOLO_MODE", "HERMES_TUI_GATEWAY_URL", "UV_LINK_MODE"),
    initialize={"clientCapabilities": {"elicitation": {"form": {}}}},
    files=hermes_files,
    # The program's tools are weft's plugin, written with the config.
    mcp=lambda adapter: Setup(),
    session_check=plugin_failure,
    stop=stop_daemon,
    machine_login="on this machine's own Hermes, its config.yaml may borrow Claude Code's and Codex's logins, and its "
                  "gateway daemon is shared with every other Hermes run here",
    program_tool=acp.ProgramTool(r"(?P<name>[^:]+?)(?:: .*)?"),
))
