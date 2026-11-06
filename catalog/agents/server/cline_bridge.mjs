// weft's bridge to Cline's SDK, started by the agent server
// (`adapter_cline.py`) as `node cline_bridge.mjs <the SDK's entry>`. The SDK
// (`@cline/sdk`, shipped inside the `cline` npm package) runs Cline's whole
// engine in this process; the agent server is Python, so this file stands
// between them and does as little as it can: it drives one session and hands
// everything else to the adapter, which decides what each thing means.
//
// Why the SDK and not `cline --acp`: over ACP the program's tools are a
// plugin whose tools lose to a built-in of the same name, and Cline has no
// question tool there. Through the SDK each program tool is one of the
// session's `extraTools`, which keeps its plain name and, named like a
// built-in (`read_files`, `run_commands`, ...), takes its place: the engine
// keeps one tool per name and adds the extra tools after its own (checked on
// 3.0.74: the model is offered the program's `read_files` alone). The SDK
// also takes a handler for its `ask_question` tool and one for every
// approval, so both reach weft as questions.
//
// The protocol: one JSON object per line, each way.
// SYNC: the protocol <-> catalog/agents/server/adapter_cline.py (the module docstring)
//
//   adapter -> bridge
//     {"id", "type": "start", "cwd", "session", "resume", "model", "mode",
//      "autoApprove", "effort", "tools"}
//         start the session `session` (or resume it: its messages read
//         back and handed to the new start), with `tools`
//         ([{name, description, parameters}]) as its extra tools;
//         answered {"id", "result": {"session", "provider", "model"}}
//     {"id", "type": "prompt", "text"}
//         answered {"id", "result": {"finishReason", "text"}} once the turn
//         is over
//     {"type": "answer", "ask", "response"}
//         the answer to an ask
//   bridge -> adapter
//     {"id", "result"} / {"id", "error": {"message"}}
//     {"type": "event", "event"}   an AgentEvent of the session's lead
//                                  agent, as is (an `error` event's error
//                                  as {message})
//     {"type": "ask", "ask", "kind", "request"}
//         Cline waits on this side: `kind` "approval" ({toolCallId,
//         toolName, input}, answered {approved, reason}), "question"
//         ({toolCallId, question, options}, answered {answer} with the
//         person's text, or {refused} with why nobody will), or "tool" ({toolCallId, name, input}, a call of one of the
//         program's tools, answered {output, isError}).
//
// No timeouts here: what waits on a person waits for as long as they take.
// The engine calls a tool's `execute` with no deadline of its own (only
// tools made through its `createTool` get one, 30 seconds by default), and
// its approval and question handlers are awaited as they are.

import { basename } from "node:path";
import { createInterface } from "node:readline";

// The SDK needs Node 22 or newer (its package's `engines`); an older Node
// fails deep inside it with an error that names nothing.
const NODE_MAJOR = Number(process.versions.node.split(".")[0]);
if (NODE_MAJOR < 22) {
  process.stderr.write(`Cline's SDK needs Node 22 or newer, and this is Node ${process.versions.node}\n`);
  process.exit(1);
}

const sdk = await import(process.argv[2]);

const asks = new Map();
let nextAsk = 0;
let core;
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

/** The program's tool `tool` as one of the session's extra tools: a call
 * of it is asked of the adapter, by the call id the engine's own events
 * carry. */
function programTool(tool) {
  return {
    name: tool.name,
    description: tool.description ?? "",
    inputSchema: tool.parameters ?? { type: "object", properties: {} },
    retryable: false,
    maxRetries: 0,
    async execute(input, context) {
      const answer = await ask("tool", { toolCallId: context.toolCallId, name: tool.name, input: input ?? {} });
      if (answer.isError) throw new Error(answer.output);
      return answer.output;
    },
  };
}

/** Cline's `ask_question`, asked of the adapter: the person's answer, or
 * the call failed with why nobody will answer it. */
async function askQuestion(question, options, context) {
  const answer = await ask("question", { toolCallId: context.toolCallId, question, options });
  if (answer.refused !== undefined) throw new Error(answer.refused);
  return answer.answer;
}

/** Cline's reasoning settings for weft's `effort`: none, or a level; left
 * out, the provider's own (what the person set in Cline, else its default). */
function reasoning(effort) {
  if (!effort) return {};
  if (effort === "none") return { thinking: false };
  return { thinking: true, reasoningEffort: effort };
}

/** The provider and model to run: `CLINE_PROVIDER` (a connection's
 * provider), else the one the person last used in Cline, else Cline's own;
 * the node's model, else the one kept with that provider, else the
 * provider's default. */
async function connection(model) {
  const settings = new sdk.ProviderSettingsManager();
  const provider = process.env.CLINE_PROVIDER || settings.getLastUsedProviderSettings()?.provider || "cline";
  const stored = settings.getProviderSettings(provider);
  // A key in the environment (a connection's), one Cline keeps for the
  // provider (`cline auth`), or a provider that runs on another tool's
  // login on this machine.
  if (!process.env.CLINE_API_KEY && !sdk.getPersistedProviderApiKey(provider, stored) && !sdk.isLocalAuthProvider(provider)) {
    throw new Error(`Cline has no login for its provider ${JSON.stringify(provider)} here: run \`cline auth\` on this machine, or wire a ClineCliAccess into \`access\``);
  }
  const id = model || stored?.model || (await sdk.Llms.getProvider(provider))?.defaultModelId;
  if (!id) throw new Error(`Cline's provider ${JSON.stringify(provider)} has no default model: set the node's \`model\``);
  return { provider, model: id };
}

async function start(m) {
  const { provider, model } = await connection(m.model);
  // The program handed its tools over: calling one is never a question,
  // and asking whether to ask the person a question would be one too many.
  // Every other tool asks, unless the permission setting approves them all.
  const toolPolicies = {
    "*": { autoApprove: m.autoApprove },
    ask_question: { autoApprove: true },
    ...Object.fromEntries(m.tools.map((t) => [t.name, { autoApprove: true }])),
  };
  core = await sdk.ClineCore.create({
    clientName: "weft",
    // In this process: extra tools and the handlers are this process's
    // own, and a shared hub would outlive the conversation.
    backendMode: "local",
    capabilities: {
      toolExecutors: { askQuestion: askQuestion },
      requestToolApproval: (request) => ask("approval", { toolCallId: request.toolCallId, toolName: request.toolName, input: request.input }),
    },
  });
  core.subscribe((event) => {
    if (event.type !== "agent_event" || event.payload?.sessionId !== m.session) return;
    const agent = event.payload.event;
    // The lead agent's alone: a sub-agent's (one with a parent agent) or a
    // teammate's would read as the conversation's own text and tool calls.
    if (agent.parentAgentId || event.payload.teamRole === "teammate") return;
    write({ type: "event", event: agent.type === "error" ? { ...agent, error: { message: messageOf(agent.error) } } : agent });
  });
  let initialMessages;
  if (m.resume) {
    initialMessages = await core.readMessages(m.session);
    if (!initialMessages || initialMessages.length === 0) throw new Error(`Cline has no conversation ${m.session} any more`);
  }
  const metadata = await sdk.buildWorkspaceMetadata(m.cwd);
  const systemPrompt = sdk.getClineDefaultSystemPrompt({
    ide: "Terminal Shell", workspaceRoot: m.cwd, workspaceName: basename(m.cwd), metadata, rules: sdk.mergeRulesForSystemPrompt(undefined, undefined),
    mode: m.mode, providerId: provider, platform: process.platform,
  });
  const started = await core.start({
    source: sdk.SessionSource.CLI,
    config: {
      sessionId: m.session, providerId: provider, modelId: model, apiKey: process.env.CLINE_API_KEY || undefined, systemPrompt,
      cwd: m.cwd, workspaceRoot: m.cwd, mode: m.mode, enableTools: true, enableSpawnAgent: true, enableAgentTeams: false,
      extraTools: m.tools.map(programTool), ...reasoning(m.effort),
      extensionContext: { client: { name: "weft", version: "1", platform: "cli" },
                          workspace: { rootPath: m.cwd, cwd: m.cwd, workspaceName: m.cwd, ide: "Terminal Shell", platform: process.platform } },
    },
    toolPolicies,
    interactive: true,
    initialMessages,
  });
  session = started.sessionId;
  return { session, provider, model };
}

/** An error's message: an Error turns into `{}` as JSON. */
function messageOf(error) {
  return (typeof error === "object" && error !== null && error.message) || String(error);
}

async function handle(m) {
  if (m.type === "start") return start(m);
  if (session === undefined) throw new Error(`${m.type} before start`);
  if (m.type === "prompt") {
    const result = await core.send({ sessionId: session, prompt: m.text });
    return { finishReason: result?.finishReason ?? "", text: result?.text ?? "" };
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
    (error) => write({ id: m.id, error: { message: messageOf(error) } }),
  );
}
// The adapter closed our input: it is done with this session.
await core?.dispose();
process.exit(0);
