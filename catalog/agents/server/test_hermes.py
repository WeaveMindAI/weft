"""Hermes Agent's gateway daemon, stopped with the conversation's agent,
and weft's plugin that gives it the program's tools.

The turns, settings and logins are in `test_acp_agents.py`, played from
`testing/recorded/hermes.json`; these are what a recording cannot show:
what `hermes acp` leaves running once it is gone, and what the plugin
files hold and do inside Hermes.

Run with the others: `python3 -m unittest test_hermes`.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "testing"))

import rig  # noqa: E402,F401 (the server's modules on the path, its logs quiet)
import acp_hermes  # noqa: E402
import adapter_acp  # noqa: E402
import adapters  # noqa: E402
import types  # noqa: E402
from events import Refused  # noqa: E402
from route_stub import RouteStub  # noqa: E402

def start_time(pid):
    """How Hermes reads a process's start time where there is a `/proc`
    (`gateway/status.py`): field 22 of `/proc/<pid>/stat`, after the
    command's last ")"."""
    with open(f"/proc/{pid}/stat", "rb") as f:
        return int(f.read().rsplit(b")", 1)[1].split()[19])


def birth(pid):
    """What Hermes records as `start_time` where there is no `/proc`: the
    process's birth in hundredths of a second since the epoch (read here
    from `/proc`, independent of the `ps` the adapter reads it from)."""
    with open("/proc/stat") as f:
        boot = next(int(line.split()[1]) for line in f if line.startswith("btime "))
    return round((boot + start_time(pid) / os.sysconf("SC_CLK_TCK")) * 100)


class Home:
    """What `stop_daemon` reads of an adapter: its private home."""

    def __init__(self, home):
        self.home = home


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


class StopDaemon(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home, True)

    def recorded_start(self, pid: int) -> int:
        """The `start_time` Hermes records for `pid` on this machine."""
        return start_time(pid)

    # How far a recorded start must be from `pid`'s for it to be another process's.
    ANOTHER_START = 1

    def daemon(self) -> tuple:
        """A stand-in gateway the way `hermes acp` leaves one: started in a
        session of its own by a process that is gone, with a child of its
        own; its record in `gateway.pid` as Hermes writes it."""
        child_pid = os.path.join(self.home, "child.pid")
        gateway = ("import subprocess, time; c = subprocess.Popen(['sleep', '600']); "
                   f"open({child_pid!r}, 'w').write(str(c.pid)); time.sleep(600)")
        launcher = (f"import json, subprocess, sys; d = subprocess.Popen([sys.executable, '-c', {gateway!r}, 'gateway', 'run'], "
                    "start_new_session=True); "
                    f"json.dump({{'pid': d.pid}}, open({self.pidfile()!r}, 'w'))")
        subprocess.run([sys.executable, "-c", launcher], check=True)
        with open(self.pidfile(), encoding="utf-8") as f:
            daemon = json.load(f)["pid"]
        self.write_pidfile({"pid": daemon, "kind": "hermes-gateway", "start_time": self.recorded_start(daemon)})
        deadline = time.monotonic() + 10
        while not os.path.exists(child_pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        with open(child_pid, encoding="utf-8") as f:
            return daemon, int(f.read())

    def pidfile(self) -> str:
        return os.path.join(self.home, "gateway.pid")

    def write_pidfile(self, record) -> None:
        with open(self.pidfile(), "w", encoding="utf-8") as f:
            f.write(record if isinstance(record, str) else json.dumps(record))

    def gone(self, pid: int) -> bool:
        deadline = time.monotonic() + 5
        while alive(pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        return not alive(pid)

    def test_the_daemon_and_what_it_started_are_stopped(self):
        daemon, child = self.daemon()
        started = time.monotonic()
        acp_hermes.stop_daemon(Home(self.home))
        self.assertLess(time.monotonic() - started, acp_hermes.DAEMON_GRACE_SECS, "it waited out the grace: SIGTERM did not stop it")
        self.assertTrue(self.gone(daemon), "the daemon outlived the stop")
        self.assertTrue(self.gone(child), "the daemon's own child outlived it")
        self.assertFalse(os.path.exists(self.pidfile()), "a stopped gateway's pid file stayed")
        acp_hermes.stop_daemon(Home(self.home))

    def test_a_home_with_no_daemon_is_left_alone(self):
        acp_hermes.stop_daemon(Home(self.home))

    def test_a_daemon_already_gone_is_left_alone(self):
        done = subprocess.Popen([sys.executable, "-c", "pass"])
        done.wait()
        self.write_pidfile({"pid": done.pid, "kind": "hermes-gateway", "start_time": 1})
        acp_hermes.stop_daemon(Home(self.home))
        self.assertFalse(os.path.exists(self.pidfile()))

    def test_a_pid_now_another_process_is_never_signalled(self):
        # The gateway was killed and its number went to another process: the
        # file's start time is no longer that pid's.
        other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)", "gateway", "run"])
        self.addCleanup(lambda: (other.kill(), other.wait()))
        self.write_pidfile({"pid": other.pid, "kind": "hermes-gateway",
                            "start_time": self.recorded_start(other.pid) - self.ANOTHER_START})
        acp_hermes.stop_daemon(Home(self.home))
        self.assertIsNone(other.poll(), "a process the pid file no longer names was killed")
        self.assertFalse(os.path.exists(self.pidfile()))

    def test_a_pid_that_is_no_gateway_is_never_signalled(self):
        other = subprocess.Popen(["sleep", "600"])
        self.addCleanup(lambda: (other.kill(), other.wait()))
        self.write_pidfile({"pid": other.pid, "kind": "hermes-gateway", "start_time": self.recorded_start(other.pid)})
        acp_hermes.stop_daemon(Home(self.home))
        self.assertIsNone(other.poll(), "a process whose command line is no gateway was killed")

    def test_a_record_with_no_start_time_is_refused(self):
        other = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)", "gateway", "run"])
        self.addCleanup(lambda: (other.kill(), other.wait()))
        self.write_pidfile({"pid": other.pid, "kind": "hermes-gateway"})
        with self.assertRaises(Refused) as refused:
            acp_hermes.stop_daemon(Home(self.home))
        self.assertIn("records no start time", str(refused.exception))
        self.assertIsNone(other.poll(), "a process the file cannot be matched to was killed")

    def test_a_malformed_pid_file_fails_loudly(self):
        for record in ("not json", json.dumps({"pid": 1}), json.dumps({"kind": "hermes-gateway", "pid": "1"})):
            with self.subTest(record=record):
                self.write_pidfile(record)
                with self.assertRaises(Refused):
                    acp_hermes.stop_daemon(Home(self.home))


class StopDaemonWithProc(StopDaemon):
    def test_a_proc_that_cannot_be_read_is_refused(self):
        self.write_pidfile({"pid": os.getpid(), "kind": "hermes-gateway", "start_time": 1})
        real_open = open

        def hidden(name, *args, **kwargs):
            if str(name).startswith(f"/proc/{os.getpid()}/"):
                raise PermissionError(13, "Permission denied", name)
            return real_open(name, *args, **kwargs)

        with unittest.mock.patch("builtins.open", hidden), self.assertRaises(Refused) as refused:
            acp_hermes.stop_daemon(Home(self.home))
        self.assertIn(f"cannot read /proc/{os.getpid()}", str(refused.exception))


class StopDaemonWithoutProc(StopDaemon):
    """The same, on a machine with no `/proc` (macOS): the start and the
    command line come from `ps`, against what Hermes records there."""

    # A minute, in Hermes' hundredths of a second: well past `ps`'s slack.
    ANOTHER_START = 60 * 100

    def setUp(self):
        super().setUp()
        patch = unittest.mock.patch.object(acp_hermes, "PROC", os.path.join(self.home, "no-proc"))
        patch.start()
        self.addCleanup(patch.stop)

    def recorded_start(self, pid: int) -> int:
        return birth(pid)

    def test_a_ps_that_is_not_there_is_refused(self):
        self.write_pidfile({"pid": os.getpid(), "kind": "hermes-gateway", "start_time": birth(os.getpid())})
        with unittest.mock.patch.dict(os.environ, {"PATH": self.home}), self.assertRaises(Refused) as refused:
            acp_hermes.stop_daemon(Home(self.home))
        self.assertIn("cannot run `ps`", str(refused.exception))


def program_tool(name: str) -> dict:
    return {"name": name, "description": f"The program's {name}", "parameters": {"type": "object", "properties": {"q": {"type": "string"}}}}


class Adapter:
    """What `hermes_config` and `plugin_failure` read of an adapter."""

    def __init__(self, home, tools):
        self.home = home
        self.tools = tools
        self.program_tools = adapters.ProgramTools(tools)
        self.config = {"model": "some/model"}
        self.credentials = {"way": "openRouter", "values": {"key": "k"}}
        self.permission = "ask"
        self.stderr = ["2026 [WARNING] hermes_cli.plugins: Failed to load plugin 'weft': boom", "unrelated"]

    def stderr_tail(self) -> str:
        return "\n".join(self.stderr)


class PluginFiles(unittest.TestCase):
    """What weft writes into the private home for the program's tools."""

    def setUp(self):
        self.home = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.home, True)
        adapters.SERVER_URL = "http://127.0.0.1:4100"

    def test_the_program_tools_are_a_plugin_allowed_to_replace_a_built_in(self):
        tools = [program_tool("lookup"), program_tool("web_search")]
        files = acp_hermes.hermes_config(Adapter(self.home, tools))
        config = json.loads(files["config.yaml"])
        self.assertEqual(config["platform_toolsets"]["acp"], ["hermes-acp", "clarify", "weft"])
        self.assertEqual(config["plugins"], {"enabled": ["weft"], "entries": {"weft": {"allow_tool_override": True}}})
        with open(acp_hermes.PLUGIN_FILE, encoding="utf-8") as f:
            self.assertEqual(files["plugins/weft/__init__.py"], f.read())
        self.assertEqual(files["plugins/weft/server.path"], os.path.dirname(acp_hermes.PLUGIN_FILE))
        self.assertEqual(json.loads(files["plugins/weft/plugin.yaml"])["provides_tools"], ["lookup", "web_search"])
        listed = json.loads(files["plugins/weft/tools.json"])
        self.assertEqual(listed["url"], "http://127.0.0.1:4100/mcp")
        self.assertEqual([t["name"] for t in listed["tools"]], ["lookup", "web_search"])
        self.assertEqual(files[acp_hermes.PLUGIN_LOADED], "", "emptied at each start, for the plugin to fill")

    def test_no_program_tools_writes_no_plugin(self):
        files = acp_hermes.hermes_config(Adapter(self.home, []))
        self.assertEqual(list(files), ["config.yaml"])
        config = json.loads(files["config.yaml"])
        self.assertNotIn("plugins", config)
        self.assertNotIn("weft", config["platform_toolsets"]["acp"])

    def test_hermes_publishes_no_command_into_the_persons_home(self):
        config = json.loads(acp_hermes.hermes_config(Adapter(self.home, []))["config.yaml"])
        self.assertEqual(config["cli"], {"expose_on_path": False},
                         "a launch under a home Hermes was not installed in writes `hermes` shims into ~/.local/bin")

    def test_a_plugin_that_did_not_load_refuses_the_session_saying_why(self):
        adapter = Adapter(self.home, [program_tool("lookup")])
        os.makedirs(os.path.join(self.home, "plugins", "weft"))
        loaded = os.path.join(self.home, acp_hermes.PLUGIN_LOADED)
        for content in ("", json.dumps(["other"])):
            with self.subTest(content=content):
                with open(loaded, "w", encoding="utf-8") as f:
                    f.write(content)
                failure = acp_hermes.plugin_failure(adapter)
                self.assertIn("did not load weft's plugin", failure)
                self.assertIn("Failed to load plugin 'weft': boom", failure)
                self.assertNotIn("unrelated", failure)
        with open(loaded, "w", encoding="utf-8") as f:
            json.dump(["lookup"], f)
        self.assertIsNone(acp_hermes.plugin_failure(adapter))
        self.assertIsNone(acp_hermes.plugin_failure(Adapter(self.home, [])), "no tools, nothing to load")

    def test_a_call_is_named_by_the_title_hermes_gives_it(self):
        # Real titles of 0.22.6: `<name>` with no arguments, `<name>: <preview>`
        # with some, the preview of a built-in's own formatter for a name it
        # has one for.
        read = adapters.KINDS["hermes"].spec.program_tool.read
        names = {"lookup", "web_search"}
        self.assertEqual(read({"title": "lookup"}, names), "lookup")
        self.assertEqual(read({"title": "lookup: weft"}, names), "lookup")
        self.assertEqual(read({"title": "web_search: weft"}, names), "web_search")
        self.assertIsNone(read({"title": "terminal: ls -la"}, names), "a built-in the program has no tool of")
        self.assertIsNone(read({"title": "mcp__editor_weft_6f300e5f__lookup"}, names))


class SharedUvCache(unittest.TestCase):
    """The conversation home's uv cache, a link to weft's install's
    (`share_uv_cache`), never into a Hermes of the person's own."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)
        self.home = os.path.join(self.root, "conversation")
        os.makedirs(self.home)

    def program(self, folder):
        os.makedirs(os.path.join(folder, "bin"), exist_ok=True)
        path = os.path.join(folder, "bin", "hermes")
        with open(path, "w", encoding="utf-8") as f:
            f.write("#!/bin/sh\n")
        os.chmod(path, 0o755)
        return types.SimpleNamespace(home=self.home, program=lambda: path)

    def cache(self):
        return os.path.join(self.home, "cache", "uv")

    def test_weft_s_install_shares_its_cache_with_every_conversation(self):
        install = os.path.join(self.root, "agents", "hermes")
        os.makedirs(os.path.join(install, "hermes", "hermes-agent"))
        adapter = self.program(install)
        acp_hermes.share_uv_cache(adapter)
        shared = os.path.join(os.path.realpath(install), "hermes", "cache", "uv")
        self.assertEqual(os.readlink(self.cache()), shared)
        self.assertTrue(os.path.isdir(shared))
        acp_hermes.share_uv_cache(adapter)
        self.assertEqual(os.readlink(self.cache()), shared, "a second start keeps the link")

    def test_a_home_s_own_cache_from_before_gives_way_to_the_shared_one(self):
        install = os.path.join(self.root, "agents", "hermes")
        os.makedirs(os.path.join(install, "hermes", "hermes-agent"))
        os.makedirs(os.path.join(self.cache(), "wheels-v7"))
        acp_hermes.share_uv_cache(self.program(install))
        self.assertTrue(os.path.islink(self.cache()))

    def test_a_hermes_of_the_person_s_own_is_never_linked_into(self):
        acp_hermes.share_uv_cache(self.program(os.path.join(self.root, "dot-local")))
        self.assertFalse(os.path.lexists(self.cache()))


class Plugin(unittest.TestCase):
    """`plugin_hermes.py` run as Hermes runs it: `register(ctx)` of the
    folder the adapter wrote, against stand-ins of Hermes' registry."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root, True)
        self.route = RouteStub("found it")
        self.addCleanup(self.route.close)
        self.folder = os.path.join(self.root, "weft")
        os.makedirs(self.folder)
        shutil.copy(acp_hermes.PLUGIN_FILE, os.path.join(self.folder, "__init__.py"))
        with open(os.path.join(self.folder, "server.path"), "w", encoding="utf-8") as f:
            f.write(os.path.dirname(acp_hermes.PLUGIN_FILE))
        with open(os.path.join(self.folder, "tools.json"), "w", encoding="utf-8") as f:
            json.dump({"url": self.route.url, "tools": [program_tool("lookup"), program_tool("web_search")]}, f)
        self.entries = {}
        registry = types.SimpleNamespace(get_entry=lambda name: self.entries[name])
        modules = {"tools": types.ModuleType("tools"), "tools.registry": types.SimpleNamespace(registry=registry),
                   "agent": types.ModuleType("agent"), "agent.deadline": types.SimpleNamespace(MAX_SAFE_TIMEOUT_S=31536000.0)}
        patch = unittest.mock.patch.dict(sys.modules, modules)
        patch.start()
        self.addCleanup(patch.stop)

    def load(self):
        import importlib.util  # noqa: PLC0415
        spec = importlib.util.spec_from_file_location("hermes_plugins.weft", os.path.join(self.folder, "__init__.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def ctx(self, handle=True):
        registered = []

        def register_tool(**kwargs):
            registered.append(kwargs)
            self.entries[kwargs["name"]] = types.SimpleNamespace(execution_timeout=None)
            return object() if handle else None

        return types.SimpleNamespace(register_tool=register_tool), registered

    def test_every_tool_is_registered_under_its_name_in_the_place_of_a_built_in(self):
        ctx, registered = self.ctx()
        self.load().register(ctx)
        self.assertEqual([(r["name"], r["toolset"], r["override"]) for r in registered],
                         [("lookup", "weft", True), ("web_search", "weft", True)])
        self.assertEqual(registered[0]["schema"]["parameters"], program_tool("lookup")["parameters"])
        self.assertEqual({name: e.execution_timeout for name, e in self.entries.items()},
                         {"lookup": 31536000.0, "web_search": 31536000.0}, "no deadline of Hermes' own on a call")
        with open(os.path.join(self.folder, "loaded.json"), encoding="utf-8") as f:
            self.assertEqual(json.load(f), ["lookup", "web_search"])

    def test_a_call_is_forwarded_to_the_route_and_its_failure_is_hermes_error(self):
        ctx, registered = self.ctx()
        self.load().register(ctx)
        self.assertEqual(registered[1]["handler"]({"q": "weft"}, task_id="t1"), "found it")
        self.assertEqual(self.route.called(), [("web_search", {"q": "weft"})])
        self.route.is_error = True
        self.assertEqual(json.loads(registered[0]["handler"]({})), {"error": "found it"})

    def test_a_registration_hermes_refuses_fails_the_plugin(self):
        ctx, _ = self.ctx(handle=False)
        with self.assertRaisesRegex(RuntimeError, "did not register the program's tool 'lookup'"):
            self.load().register(ctx)
        self.assertFalse(os.path.exists(os.path.join(self.folder, "loaded.json")))


del StopDaemon


if __name__ == "__main__":
    unittest.main()
