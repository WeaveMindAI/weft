// weft's bridge to Kimi Code's SDK, started by the agent server
// (`adapter_kimi.py`) as `kimi-code-sdk kimi_bridge.mjs`. Kimi Code's SDK is
// JavaScript and runs Kimi's whole engine in this process; the agent server
// is Python, so this file stands between them and does as little as it can:
// it drives one session and hands everything else to the adapter, which
// decides what each thing means.
//
// `kimi-code-sdk` is a build of the SDK and its engine (`agents.json`,
// `kimi.install`) that loads this module and calls `main(sdk, engine)`, so
// the session and the engine services this file reaches into are one engine.
// SYNC: the host's contract `main(sdk, engine, args)` <-> catalog/agents/server/agents.json (kimi.install, weft-host.ts)
//
// Why the engine and not only the SDK: the program's tools are Kimi "user
// tools", which keep the name they are given and, registered under a
// built-in's name, take its place (Kimi's tool registry keeps one tool per
// name, the last registered). The SDK has no call that registers one and
// answers every call of one with an error, so this file registers them on
// the session's main agent itself and answers their calls (`toolCall`).
//
// The protocol: one JSON object per line, each way.
// SYNC: the protocol <-> catalog/agents/server/adapter_kimi.py (the module docstring)
//
//   adapter -> bridge
//     {"id", "type": "start", "workDir", "session", "resume", "model",
//      "thinking", "permission", "plan", "additionalDirs", "tools"}
//         create the session `session` (or resume it), set it up, register
//         `tools` ([{name, description, parameters}]) as its user tools;
//         answered {"id", "result": {"session"}}
//     {"id", "type": "tools"}
//         answered {"id", "result": {"tools": [{name, source}]}}: the main
//         agent's tools as its registry holds them now
//     {"id", "type": "prompt", "text"}
//         answered {"id", "result": {}} once Kimi took the message; the
//         turn itself ends with the `turn.ended` event
//     {"type": "answer", "ask", "response"}
//         the answer to an ask
//   bridge -> adapter
//     {"id", "result"} / {"id", "error": {"message"}}
//     {"type": "event", "event"}       an SDK event of the main agent, as is
//     {"type": "ask", "ask", "kind", "request"}
//         Kimi waits on this side: `kind` "approval" (an ApprovalRequest,
//         answered with an ApprovalResponse), "question" (a QuestionRequest,
//         answered with a QuestionResult), or "tool" ({toolCallId, name,
//         args}, a call of one of the program's tools, answered with
//         {output, isError}). An ask waits for as long as its answer takes.
//
// No timeouts here: what waits on a person waits for as long as they take.

import { createInterface } from "node:readline";

export async function main(sdk, engine) {
  const asks = new Map();
  let nextAsk = 0;
  let session;

  const write = (message) => process.stdout.write(JSON.stringify(message) + "\n");

  /** Hand `request` to the adapter and wait for its answer. */
  function ask(kind, request) {
    const id = `ask-${++nextAsk}`;
    return new Promise((resolve) => {
      asks.set(id, resolve);
      write({ type: "ask", ask: id, kind, request });
    });
  }

  // The SDK's client, with the one call the SDK leaves unanswered answered:
  // a call of a user tool. Its request names no tool, so the name is read
  // off the engine's own pending call of that tool call id.
  class Client extends sdk.SDKRpcClientV2 {
    async toolCall(request) {
      const pending = engine.interactions.findAll({
        resolved: false,
        kind: "user_tool",
        tags: { [engine.INTERACTION_TAG_TOOL_CALL_ID]: request.toolCallId },
      });
      if (pending.length !== 1) {
        return { output: `weft found ${pending.length} pending calls for tool call ${request.toolCallId}, so it cannot tell which tool this is`, isError: true };
      }
      return ask("tool", { toolCallId: request.toolCallId, name: pending[0].payload.name, args: request.args });
    }
  }

  // As `sdk.createKimiHarness` builds its harness, on the client above. The
  // SDK asks every host to name itself to Moonshot's endpoints.
  const rpc = new Client({ identity: { productName: "weft", version: "1", platform: "weft" } });
  rpc.suppressEngineSessionStarted();
  const harness = new sdk.KimiHarness(rpc, {
    identity: rpc.identity,
    homeDir: rpc.homeDir,
    configPath: rpc.configPath,
    auth: rpc.auth,
    telemetry: rpc.telemetry,
    ensureConfigFile: () => rpc.ensureConfigFile(),
    onClose: () => rpc.close(),
    imageLimits: undefined,
  });

  async function start(m) {
    const options = { id: m.session, workDir: m.workDir, additionalDirs: m.additionalDirs ?? [] };
    if (m.resume) {
      session = await harness.resumeSession(options);
      // A resumed session comes back as it was left: each setting is set again.
      await session.setPermission(m.permission);
      await session.setPlanMode(m.plan);
      if (m.model) await session.setModel(m.model);
      if (m.thinking) await session.setThinking(m.thinking);
    } else {
      session = await harness.createSession({ ...options, model: m.model || undefined, thinking: m.thinking || undefined, permission: m.permission, planMode: m.plan });
    }
    session.onEvent((event) => {
      if (event.agentId === engine.MAIN_AGENT_ID) write({ type: "event", event });
    });
    session.setApprovalHandler((request) => ask("approval", request));
    session.setQuestionHandler((request) => ask("question", request));
    await setTools(m.tools);
    return { session: session.id };
  }

  // The program's tools, and only them. A resumed session restores the ones
  // it had, so one the program no longer has is taken out, and a built-in
  // it had replaced is put back: taking a user tool out removes its name
  // from the registry and from the agent's active tools, and the engine's
  // activation registers a built-in only where the name is free and active.
  async function setTools(wanted) {
    const agent = mainAgent();
    const tools = agent.accessor.get(engine.IAgentUserToolService);
    const profile = agent.accessor.get(engine.IAgentProfileService);
    const registry = agent.accessor.get(engine.IAgentToolRegistryService);
    const names = new Set(wanted.map((tool) => tool.name));
    const freed = tools.list().map((old) => old.name).filter((name) => !names.has(name));
    for (const name of freed) {
      tools.unregister(name);
      profile.addActiveTool(name);
    }
    if (freed.length > 0) await agent.accessor.get(engine.IAgentToolActivationService).activate();
    // A freed name no built-in took back was a tool of the program's alone.
    for (const name of freed) {
      if (registry.resolve(name) === undefined) profile.removeActiveTool(name);
    }
    for (const tool of wanted) {
      tools.register({ name: tool.name, description: tool.description ?? "", parameters: tool.parameters ?? { type: "object", properties: {} } });
    }
  }

  function mainAgent() {
    const live = engine.getLiveSessionById(rpc.engineAccessor, session.id);
    const agent = live?.accessor.get(engine.IAgentLifecycleService).handleOf(engine.MAIN_AGENT_ID);
    if (agent === undefined) throw new Error(`Kimi has no main agent in session ${session.id}`);
    return agent;
  }

  function registered() {
    return { tools: mainAgent().accessor.get(engine.IAgentToolRegistryService).list().map(({ name, source }) => ({ name, source })) };
  }

  async function handle(m) {
    if (m.type === "start") return start(m);
    if (session === undefined) throw new Error(`${m.type} before start`);
    if (m.type === "tools") return registered();
    if (m.type === "prompt") {
      await session.prompt(m.text);
      return {};
    }
    throw new Error(`the bridge has no request ${JSON.stringify(m.type)}`);
  }

  for await (const line of createInterface({ input: process.stdin })) {
    if (!line.trim()) continue;
    const m = JSON.parse(line);
    if (m.type === "answer") {
      const resolve = asks.get(m.ask);
      asks.delete(m.ask);
      if (resolve === undefined) throw new Error(`an answer to ${m.ask}, which nothing asked`);
      resolve(m.response);
      continue;
    }
    handle(m).then(
      (result) => write({ id: m.id, result }),
      (error) => write({ id: m.id, error: { message: error?.message ?? String(error) } }),
    );
  }
  // The adapter closed our input: it is done with this session.
  await harness.close();
  process.exit(0);
}
