// weft's plugin for OpenClaw. Its loader (`js_plugin.py`) is the `index.js`
// of a plugin folder in the conversation's private home, beside the
// manifest that declares every tool (`contracts.tools`), and
// `plugins.load.paths` names the folder. It does two things:
//
//   - the program's tools, each under its plain name, forwarded to the
//     agent server's MCP route (`weft_tools.mjs`). An MCP server's tools are
//     `<server>__<tool>`; a plugin's tool keeps the name it registers.
//     OpenClaw skips a plugin tool named like one of its core tools, so the
//     node refuses those names when the program is built
//     (`reservedToolNames`), and none reaches here.
//   - the longest wait OpenClaw allows on its own question tool. The model
//     picks how long `ask_user` waits for the person (15 minutes when it
//     says nothing), and the question is dropped after that; a person
//     answering through weft may take far longer, so every call asks for
//     OpenClaw's maximum instead, an hour, which OpenClaw fixes in its code.
//
// SYNC: ASK_TOOL, ASK_MAX_SECS <-> catalog/agents/server/adapter_openclaw.py (the module's doc)

import { callTool, schemaOf } from "./weft_tools.mjs";

const ASK_TOOL = "ask_user";
// OpenClaw's own cap (`MAX_ASK_USER_TIMEOUT_SECONDS`, 2026.11.1): a larger
// value is clamped to it.
const ASK_MAX_SECS = 3600;

export function make({ url, tools }) {
  return {
    id: "weft",
    name: "weft",
    description: "The tools of the weft program this agent works for.",
    register(api) {
      for (const tool of tools) {
        api.registerTool({
          name: tool.name,
          label: tool.name,
          description: tool.description,
          parameters: schemaOf(tool),
          async execute(_toolCallId, params, signal) {
            const answer = await callTool(url, tool.name, params, signal);
            if (answer.isError) throw new Error(answer.text);
            return { content: [{ type: "text", text: answer.text }], details: {} };
          },
        });
      }
      api.on("before_tool_call", (event) => {
        if (event.toolName !== ASK_TOOL) return undefined;
        return { params: { ...event.params, timeoutSeconds: ASK_MAX_SECS } };
      });
    },
  };
}
