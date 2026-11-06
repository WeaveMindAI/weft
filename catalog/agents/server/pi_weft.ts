// weft's extension for Pi, loaded with `-e` by the agent server
// (`adapter_pi.py`) through its loader in the conversation's Pi home
// (`js_plugin.py`), which hands it the program's tools. Pi itself asks
// nothing before a tool and has no tool for asking a person, so this file
// adds both, and the program's tools:
//
//   - before each tool call, under the `ask` and `accept_edits` permission
//     settings (`--weft-permission`), it asks the client and waits;
//   - the `ask_user` tool, whose question reaches the client and whose
//     answer comes back as the tool's result;
//   - each of the program's tools, under its plain name (an MCP server's
//     would be `mcp__weft__<tool>`), its calls forwarded to the agent
//     server's MCP route (`weft_tools.mjs`). A tool an extension registers
//     takes the place of Pi's built-in of that name (`read`, `bash`, ...),
//     and of the built-in `codemode` and `tool_search` extensions, so a
//     program tool may take any name, `ask_user` included (weft's own is
//     then left out).
//
// Both go out as one of Pi's own RPC dialogs (`ctx.ui.select`,
// `ctx.ui.input`), an `extension_ui_request` the client answers with an
// `extension_ui_response`. The dialog's title is a JSON object starting
// with `"weft"`, so the adapter reads what is asked without parsing
// prose, and a permission's answer is a JSON object too: `{choice, note}`.
// No dialog carries a timeout: a person may answer days later.

import { realpathSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, isAbsolute, relative, resolve } from "node:path";
import { callTool, schemaOf } from "./weft_tools.mjs";

// SYNC: these names <-> catalog/agents/server/adapter_pi.py (ASK_TOOL, PERMISSION_FLAG, CHOICES, PERMISSIONS)
const ASK_TOOL = "ask_user";
const PERMISSION_FLAG = "weft-permission";
const CHOICES = ["allow_once", "allow_always", "reject"];
const PERMISSIONS = ["ask", "accept_edits", "read_only", "bypass"];

// Pi's built-in tools that only read, and the ones that change a file.
const READS = new Set(["read", "grep", "find", "ls"]);
const EDITS = new Set(["edit", "write"]);
// A custom session entry per tool allowed "from now on", so a resumed
// conversation still has them.
const ALLOWED_ENTRY = "weft-allowed";

// What a call of the program's tool gives back in a built-in's place: Pi's
// session and renderers read a built-in's result by its `details` type, so
// a replacement returns that type, holding nothing (no truncation, no diff;
// the program's tool says what it did in its text). Every other tool's is
// empty.
const BUILT_IN_DETAILS: Record<string, unknown> = {
  read: {},
  bash: {},
  powershell: {},
  grep: {},
  find: {},
  ls: {},
  edit: { diff: "", patch: "" },
  write: undefined,
};

/** The file or folder a built-in tool acts on, resolved as Pi resolves it. */
function target(tool: string, input: any, cwd: string): string | undefined {
  if (!READS.has(tool) && !EDITS.has(tool)) return undefined;
  let path = typeof input?.path === "string" && input.path !== "" ? input.path : ".";
  if (path.startsWith("@")) path = path.slice(1);
  if (path === "~" || path.startsWith("~/")) path = homedir() + path.slice(1);
  return resolve(cwd, path);
}

/** The real path of `path`, through its deepest folder that exists (a new file has none of its own). */
function real(path: string): string {
  let rest = "";
  let at = path;
  for (;;) {
    try {
      return resolve(realpathSync(at), rest);
    } catch {
      const up = dirname(at);
      if (up === at) return path;
      rest = rest ? `${at.slice(up.length + 1)}/${rest}` : at.slice(up.length + 1);
      at = up;
    }
  }
}

function inside(folder: string, path: string): boolean {
  const from = relative(real(folder), real(path));
  return from === "" || (!from.startsWith("..") && !isAbsolute(from));
}

type ProgramTool = { name: string; description: string; parameters: unknown };

export function make({ url, tools }: { url: string; tools: ProgramTool[] }) {
  // The program's tools: the program decided to hand them over, so calling
  // one is never a question, whatever its name.
  const program = new Set(tools.map((tool) => tool.name));
  return (pi: any) => extension(pi, url, tools, program);
}

function extension(pi: any, url: string, tools: ProgramTool[], program: Set<string>) {
  pi.registerFlag(PERMISSION_FLAG, {
    description: `weft's permission setting: ${PERMISSIONS.join(", ")}`,
    type: "string",
    default: "ask",
  });

  const allowed = new Set<string>();

  pi.on("session_start", async (_event: any, ctx: any) => {
    allowed.clear();
    for (const entry of ctx.sessionManager.getEntries()) {
      if (entry.type === "custom" && entry.customType === ALLOWED_ENTRY && typeof entry.data?.tool === "string") {
        allowed.add(entry.data.tool);
      }
    }
  });

  pi.on("tool_call", async (event: any, ctx: any) => {
    const permission = pi.getFlag(PERMISSION_FLAG);
    if (!PERMISSIONS.includes(permission)) {
      return { block: true, reason: `weft's Pi extension has no permission setting ${JSON.stringify(permission)}` };
    }
    // `read_only` takes the tools that change anything away (`--tools`),
    // so what is left never asks.
    if (permission === "bypass" || permission === "read_only") return undefined;
    const tool = event.toolName;
    if (program.has(tool) || tool === ASK_TOOL || allowed.has(tool)) return undefined;
    const path = target(tool, event.input, ctx.cwd);
    const here = path !== undefined && inside(ctx.cwd, path);
    if (here && READS.has(tool)) return undefined;
    if (here && permission === "accept_edits" && EDITS.has(tool)) return undefined;

    const asked = JSON.stringify({ weft: "permission", toolCallId: event.toolCallId, tool, input: event.input, path });
    const value = await ctx.ui.select(asked, CHOICES, { signal: ctx.signal });
    if (value === undefined || value === null) return { block: true, reason: "No answer came: the turn ended before the person answered." };
    let answer: any;
    try {
      answer = JSON.parse(value);
    } catch {
      return { block: true, reason: `weft answered ${JSON.stringify(value)}, which is not a permission answer.` };
    }
    if (answer.choice === "allow_once") return undefined;
    if (answer.choice === "allow_always") {
      allowed.add(tool);
      pi.appendEntry(ALLOWED_ENTRY, { tool });
      return undefined;
    }
    const note = typeof answer.note === "string" && answer.note.trim() ? `: ${answer.note.trim()}` : ".";
    return { block: true, reason: `The person refused this ${tool} call${note}` };
  });

  for (const tool of tools) {
    const details = Object.hasOwn(BUILT_IN_DETAILS, tool.name) ? BUILT_IN_DETAILS[tool.name] : {};
    pi.registerTool({
      name: tool.name,
      label: tool.name,
      description: tool.description,
      promptSnippet: tool.description,
      parameters: schemaOf(tool),
      // No deadline: Pi puts none on an extension's tool, and the call may
      // wait on a person for hours.
      async execute(_toolCallId: string, params: any, signal: any) {
        const answer = await callTool(url, tool.name, params, signal);
        return { content: [{ type: "text", text: answer.text }], details, isError: answer.isError };
      },
    });
  }

  // A program tool named like it takes its place.
  if (program.has(ASK_TOOL)) return;
  pi.registerTool({
    name: ASK_TOOL,
    label: "Ask the person",
    description:
      "Ask the person you work for a question and wait for their answer, which may take a long time. " +
      "Use it when you need a decision or a fact only they have; give `options` when the answer is one of a few choices.",
    promptSnippet: "Ask the person a question and wait for the answer",
    parameters: {
      type: "object",
      properties: {
        question: { type: "string", description: "The question, complete on its own." },
        options: { type: "array", items: { type: "string" }, description: "Answers to pick from; the person may also answer in their own words." },
        multiSelect: { type: "boolean", description: "Whether the person may pick several of `options`." },
      },
      required: ["question"],
    },
    async execute(toolCallId: string, params: any, signal: any, _onUpdate: any, ctx: any) {
      const asked = JSON.stringify({
        weft: "question",
        toolCallId,
        question: params.question,
        options: Array.isArray(params.options) ? params.options.filter((o: any) => typeof o === "string") : [],
        multiSelect: params.multiSelect === true,
      });
      const answer = await ctx.ui.input(asked, "", { signal });
      if (answer === undefined || answer === null) {
        return { content: [{ type: "text", text: "No answer came: the turn ended before the person answered." }], details: {}, isError: true };
      }
      return { content: [{ type: "text", text: answer }], details: { answer } };
    },
  });
}
