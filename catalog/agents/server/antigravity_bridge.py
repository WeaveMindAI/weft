"""weft's bridge to Google Antigravity's SDK, started by the agent server
(`adapter_antigravity.py`) as `antigravity-sdk antigravity_bridge.py`.

Antigravity's SDK (`google-antigravity` on PyPI) is Python, and needs
pydantic, protobuf and websockets; the agent server runs on the standard
library alone. So the SDK lives in an environment of its own, made by the
install (`agents.json`, `antigravity.install`), whose `antigravity-sdk`
runs this file on that environment's Python. This file drives one
conversation and hands everything else to the adapter, which decides what
each thing means: it never decides a permission, names a tool, or reads a
step.

Why the SDK, not Antigravity's ACP server (which is built on it): through
the SDK the program's tools are tools of the session under their own
names, and one named like a built-in takes the built-in's place. Over ACP
they are an MCP server's, which the model reaches only under a prefix.

The protocol: one JSON object per line, each way.
SYNC: the protocol <-> catalog/agents/server/adapter_antigravity.py (the module docstring)

  adapter -> bridge
    {"id", "type": "start", "workDir", "dirs", "conversation", "resume",
     "saveDir", "model", "effort", "systemPrompt", "key", "tools", "enabled"}
        start the conversation `conversation` (a new one, or the one kept
        under `saveDir` when `resume`); `tools` ([{name, description,
        parameters}]) are the program's, `enabled` the built-in tools left
        on (the SDK's names); `key` ({env, vertex}) names the variable a
        connection's key is in, and whether it is a Vertex AI key, or is
        null for the machine's own login; `baseUrl`, which no node sets, is
        a stand-in of Gemini's API (the tests', which needs no account);
        answered {"id", "result": {"conversation"}}
    {"id", "type": "prompt", "text"}
        answered {"id", "result": {}} once the message went; the turn ends
        with the `ended` line
    {"type": "answer", "ask", "response"}
        the answer to an ask
  bridge -> adapter
    {"id", "result"} / {"id", "error": {"message"}}
    {"type": "step", "step"}           a step of the turn, as the SDK gives
                                       it (every agent's: sub-agents' too)
    {"type": "tool_done", "result"}    a tool's result, as the SDK's post-tool
                                       hook gives it ({id, name, result, error})
    {"type": "ask", "ask", "kind", "request"}
        the SDK waits on the adapter: "pre_tool" ({id, stepId, name, args}
        before a tool runs, answered {allow, message}), "question" ({questions:
        [{question, options: [{id, text}], isMultiSelect}]}, answered {responses:
        [{selectedOptionIds, freeformResponse, skipped}], cancelled}), "tool"
        ({id, name, args}: a call of one of the program's tools, answered
        {output, isError}). An ask waits for as long as its answer takes.
    {"type": "ended", "error"}         the turn ended; `error` when it failed

No timeouts here: what waits on a person waits for as long as they take.
"""

import asyncio
import json
import os
import sys

from google import antigravity as ag
from google.antigravity import types
from google.antigravity.hooks import hooks
from google.antigravity.models import DEFAULT_MODEL
from google.antigravity.tools import tool_runner

out_lock = asyncio.Lock()
asks = {}
next_ask = [0]


async def write(message: dict) -> None:
    async with out_lock:
        sys.stdout.write(json.dumps(message) + "\n")
        sys.stdout.flush()


async def ask(kind: str, request: dict):
    """Hand `request` to the adapter and wait for its answer."""
    next_ask[0] += 1
    ask_id = f"ask-{next_ask[0]}"
    answer = asyncio.get_running_loop().create_future()
    asks[ask_id] = answer
    await write({"type": "ask", "ask": ask_id, "kind": kind, "request": request})
    return await answer


class RoutedToolRunner(tool_runner.ToolRunner):
    """The SDK's runner of the session's own tools, which are the program's:
    each call goes to the adapter with the id the model gave it, which the
    tool function the SDK would call never gets (the adapter pairs the call
    with its `pre_tool` and its `tool_done` by that id). The SDK builds its
    runner as `tool_runner.ToolRunner(...)` when the agent starts, so this
    class is put in its place before that."""

    async def process_tool_calls(self, tool_calls):
        async def one(tc):
            answer = await ask("tool", {"id": tc.id, "name": str(tc.name), "args": tc.args})
            output = str(answer.get("output", ""))
            if answer.get("isError"):
                return types.ToolResult(id=tc.id, step_id=tc.step_id, name=tc.name, error=output)
            return types.ToolResult(id=tc.id, step_id=tc.step_id, name=tc.name, result=output)
        return list(await asyncio.gather(*[one(tc) for tc in tool_calls]))


tool_runner.ToolRunner = RoutedToolRunner


def program_tool(tool: dict):
    """A program tool as the SDK declares it: its name, description and
    parameters as the program gave them. Its function is never called (the
    runner above answers its calls), and says so loudly if it is: an SDK
    that calls it no longer builds its runner the way this file expects."""
    name = tool["name"]

    def unrouted(**_):
        raise RuntimeError(f"the SDK ran the program's tool {name!r} outside weft's route; this bridge does not match "
                           "the installed google-antigravity")

    declared = tool_runner.ToolWithSchema(unrouted, tool.get("parameters") or {"type": "object", "properties": {}})
    declared.__name__ = name
    declared.__qualname__ = name
    declared.__doc__ = tool.get("description") or ""
    return declared


@hooks.pre_tool_call_decide
async def pre_tool(call: types.ToolCall):
    answer = await ask("pre_tool", {"id": call.id, "stepId": call.step_id, "name": str(call.name), "args": call.args})
    return types.HookResult(allow=bool(answer.get("allow")), message=answer.get("message") or "")


@hooks.post_tool_call
async def post_tool(result: types.ToolResult):
    await write({"type": "tool_done", "result": {"id": result.id, "stepId": result.step_id, "name": str(result.name),
                                                 "result": None if result.result is None else str(result.result),
                                                 "error": result.error}})


@hooks.on_interaction
async def question(spec: types.AskQuestionInteractionSpec):
    answer = await ask("question", {"questions": [{"question": q.question, "options": [{"id": o.id, "text": o.text} for o in q.options],
                                                   "isMultiSelect": q.is_multi_select} for q in spec.questions]})
    return types.QuestionHookResult(cancelled=bool(answer.get("cancelled")), responses=[
        types.QuestionResponse(selected_option_ids=r.get("selectedOptionIds"), freeform_response=r.get("freeformResponse") or "",
                               skipped=bool(r.get("skipped"))) for r in answer.get("responses") or []])


def endpoints(m: dict) -> dict:
    """Where the model is reached, and with which key: the connection's
    (read from the variable `key` names), the machine's own (the SDK reads
    `GEMINI_API_KEY`, or Vertex AI's variables, as it always does), or the
    stand-in at `baseUrl`."""
    model = m.get("model") or None
    if m.get("baseUrl"):
        return {"models": [ag.ModelTarget(name=model or DEFAULT_MODEL, types=[kind], endpoint=ag.GeminiAPIEndpoint(
            base_url=m["baseUrl"], api_key="stand-in")) for kind in (ag.ModelType.TEXT, ag.ModelType.IMAGE)]}
    chosen = {"model": model} if model else {}
    if m.get("key") is None:
        return chosen
    return {**chosen, "api_key": os.environ[m["key"]["env"]], "vertex": bool(m["key"]["vertex"])}


def config(m: dict) -> ag.LocalAgentConfig:
    resume = {"conversation_id": m["conversation"],
              "session_continuation_mode": types.SessionContinuationMode.RESUME if m["resume"]
              else types.SessionContinuationMode.CREATE_ONLY}
    built = ag.LocalAgentConfig(
        **endpoints(m), **resume,
        system_instructions=m.get("systemPrompt") or None,
        tools=[program_tool(t) for t in m["tools"]],
        # Every decision is the adapter's, through `pre_tool`: no policy of
        # the SDK's own (it would decide before the hook is asked).
        policies=[], hooks=[pre_tool, post_tool, question],
        workspaces=[m["workDir"], *m["dirs"]], save_dir=m["saveDir"],
        capabilities=ag.CapabilitiesConfig(agent_behavior=ag.AgentBehavior.INTERACTIVE,
                                           enabled_tools=[types.BuiltinTools(t) for t in m["enabled"]]),
    )
    if m.get("effort"):
        # The thinking level is an option of the text model's endpoint.
        for target in built.models:
            if ag.ModelType.TEXT in target.types:
                target.endpoint.options = ag.GeminiModelOptions(thinking_level=m["effort"])
    return built


class Bridge:
    def __init__(self):
        self.agent = None
        self.turn = None

    async def start(self, m: dict) -> dict:
        self.agent = ag.Agent(config(m))
        await self.agent.__aenter__()
        return {"conversation": m["conversation"]}

    async def prompt(self, m: dict) -> dict:
        if self.turn is not None and not self.turn.done():
            raise RuntimeError("a turn is still running")
        conversation = self.agent.conversation
        await conversation.send(m["text"])
        self.turn = asyncio.create_task(self.run_turn(conversation))
        return {}

    async def run_turn(self, conversation) -> None:
        error = None
        try:
            async for step in conversation.receive_steps():
                await write({"type": "step", "step": step.model_dump(mode="json", exclude_none=True)})
        except Exception as e:  # noqa: BLE001 (the turn's failure, said to the adapter)
            error = str(e) or type(e).__name__
        await write({"type": "ended", "error": error})

    async def handle(self, m: dict) -> None:
        if m.get("type") == "answer":
            answer = asks.pop(m.get("ask"), None)
            if answer is None:
                # The two ends disagree on what is waiting: nothing after this
                # can be trusted, so the bridge stops, and the adapter fails
                # the turn with this line.
                print(f"weft's Antigravity bridge: the adapter answered {m.get('ask')!r}, which nothing asked", file=sys.stderr, flush=True)
                os._exit(1)
            answer.set_result(m.get("response") or {})
            return
        try:
            if m.get("type") == "start":
                result = await self.start(m)
            elif m.get("type") == "prompt":
                result = await self.prompt(m)
            else:
                raise RuntimeError(f"the bridge has no request {m.get('type')!r}")
        except Exception as e:  # noqa: BLE001 (said back as the request's error)
            await write({"id": m.get("id"), "error": {"message": f"{type(e).__name__}: {e}"}})
            return
        await write({"id": m["id"], "result": result})


async def main() -> None:
    bridge = Bridge()
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=1 << 26)
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    while True:
        line = await reader.readline()
        if not line:
            break
        if line.strip():
            # Each request on its own: a `start` or a `prompt` may wait while
            # an answer to an ask arrives behind it.
            asyncio.create_task(bridge.handle(json.loads(line)))
    if bridge.agent is not None:
        await bridge.agent.__aexit__(None, None, None)


if __name__ == "__main__":
    asyncio.run(main())
