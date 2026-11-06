// The program's tools, for an agent that takes tools from a plugin of its
// own (Pi's extension, Kilo Code's and OpenCode's plugins, OpenClaw's):
// such a tool keeps its plain name (`search`, never
// `mcp__weft__search`), and a call of it is forwarded here to the agent
// server's MCP route, where every call counts as the program's.
//
// The agent server writes one small loader per start into a folder the
// agent reads (`js_plugin.py`), which imports the agent's own plugin
// file and hands it the route and the program's tools:
//
//   import { make } from "file:///.../kilo_weft.mjs";
//   export default make({ url: "http://127.0.0.1:4100/mcp", tools: [...] });
//
// Plain JavaScript, so Node, Bun and every agent's TypeScript loader take it.

import { request } from "node:http";

/**
 * Call the program's tool `name` with `args` over the MCP route at `url`
 * (a JSON-RPC `tools/call`, which needs no `initialize` first), and give
 * back its answer: `{ text, isError }`.
 *
 * The call may be held for hours (a person answering), so nothing here
 * times out: `node:http` sets no timeout of its own, unlike `fetch`, whose
 * Node implementation gives up on a response that has not started within
 * five minutes. Only `signal` (the agent aborting the call) ends it early.
 * A route that cannot be reached, or answers something that is not an MCP
 * result, throws: the agent shows the call as failed, with the reason.
 */
export function callTool(url, name, args, signal) {
  const body = JSON.stringify({
    jsonrpc: "2.0",
    id: `weft-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    method: "tools/call",
    params: { name, arguments: args ?? {} },
  });
  return new Promise((resolve, reject) => {
    const req = request(
      url,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "Content-Length": Buffer.byteLength(body),
        },
        signal: signal ?? undefined,
      },
      (res) => {
        const chunks = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("error", reject);
        res.on("end", () => {
          const text = Buffer.concat(chunks).toString("utf8");
          let answer;
          try {
            answer = JSON.parse(text);
          } catch {
            reject(new Error(`weft's agent server answered the call of ${name} with HTTP ${res.statusCode}: ${text.slice(0, 500)}`));
            return;
          }
          if (answer.error) {
            reject(new Error(`weft's agent server refused the call of ${name}: ${answer.error.message ?? JSON.stringify(answer.error)}`));
            return;
          }
          const result = answer.result;
          if (!result || !Array.isArray(result.content)) {
            reject(new Error(`weft's agent server answered the call of ${name} with no MCP result: ${text.slice(0, 500)}`));
            return;
          }
          resolve({ text: textOf(result.content), isError: result.isError === true });
        });
      },
    );
    req.on("error", (error) => reject(new Error(`weft's agent server did not answer the call of ${name}: ${error.message}`)));
    req.end(body);
  });
}

/** The text of an MCP result's content. */
function textOf(content) {
  return content.map((part) => (part?.type === "text" ? part.text ?? "" : `[${part?.type ?? "content"}]`)).join("\n");
}

/** A program tool's parameters: always a JSON Schema object. */
export function schemaOf(tool) {
  const schema = tool.parameters;
  return schema && typeof schema === "object" && !Array.isArray(schema) ? schema : { type: "object", properties: {} };
}
