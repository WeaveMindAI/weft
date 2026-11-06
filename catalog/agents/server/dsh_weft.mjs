// weft's plugin for DeepSeek Harness: the program's tools, each under its
// plain name. A patch file of weft's own (`dsh --patch`, `acp_dsh.py`)
// inserts it into the `acp` profile with its config: the MCP route and the
// program's tools (`program_tools.listing`).
//
// dsh shows an MCP server's tools as `mcp__<server>__<tool>`. A tool a
// plugin registers into `ctx.tools` keeps the name it gives. Registered
// through an agent's own context (`agent.ctx.tools`), it shadows a built-in
// of that name for that agent and the agents it starts, while the built-in's
// row stays on and keeps every other tool it brings (`dsh-tools`: "Scoped
// tools shadow globals"). So each top-level agent gets the program's tools
// as it is made (`agent/created`, a resumed session's included); a sub-agent
// sees its parent's, filtered as dsh filters what a sub-agent inherits.
//
// A tool's parameters may be a raw JSON Schema, as dsh's own MCP client
// hands an MCP server's schema over: the program's own, optional fields
// and all.

import { callTool, schemaOf } from "./weft_tools.mjs";

export const name = "weft-tools";
export const inject = ["tools", "agents"];

export function apply(ctx, { url, tools }) {
  const installed = new Map();
  const install = (agent) => {
    if (agent.parentAgent !== undefined || installed.has(agent)) return;
    const disposers = [];
    installed.set(agent, disposers);
    for (const tool of tools) {
      try {
        disposers.push(register(agent.ctx, url, tool));
      } catch (error) {
        // dsh only warns about a plugin that failed, and runs on without
        // the program's tools. Thrown outside dsh's call, the error ends
        // dsh instead, saying why (a name dsh keeps for itself).
        setImmediate(() => {
          throw new Error(`weft could not give DeepSeek Harness the program's tool ${tool.name}: ${error.message}`);
        });
        return;
      }
    }
  };
  const uninstall = (agent) => {
    for (const dispose of installed.get(agent) ?? []) dispose();
    installed.delete(agent);
  };
  for (const agent of ctx.agents.list()) install(agent);
  ctx.on("agent/created", ({ agent }) => install(agent));
  ctx.on("agent/disposed", ({ agent }) => uninstall(agent));
  ctx.effect(() => () => {
    for (const agent of [...installed.keys()]) uninstall(agent);
  }, "weft-tools.scopedTools()");
}

function register(scoped, url, tool) {
  return scoped.tools.register({
    name: tool.name,
    description: tool.description,
    parameters: schemaOf(tool),
    output: { schema: { type: "string" }, render: (_args, value) => [{ type: "text", text: value }] },
    async execute(args, exec) {
      // No limit: dsh's timeout policy cuts off only a tool its own
      // configuration gives one, and the call may wait on a person
      // for hours.
      const answer = await callTool(url, tool.name, args, exec.signal);
      if (answer.isError) throw new Error(answer.text);
      return answer.text;
    },
  });
}
