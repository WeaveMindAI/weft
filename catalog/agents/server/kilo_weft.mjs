// weft's plugin for Kilo Code: the program's tools, each under its plain
// name. Its loader (`js_plugin.py`) is named in `KILO_CONFIG_CONTENT`'s
// `plugin` list.
//
// A plugin's `tool` map keeps the names it gives (an MCP server's tools are
// `<server>_<tool>`), and Kilo puts the tools of its plugins after its own
// built-ins in the session's tool map, so a program tool named `read` is
// the `read` the model calls: Kilo's own is replaced, never denied (a
// permission that denies a name hides every tool of that name, ours too).
//
// A plugin tool's `args` are Zod, or JSON Schema properties Kilo marks all
// required; the `tool.definition` hook hands the model the program's own
// schema instead, optional fields and all.

import { callTool, schemaOf } from "./weft_tools.mjs";

export function make({ url, tools }) {
  const byName = new Map(tools.map((tool) => [tool.name, tool]));
  return async function weft() {
    return {
      tool: Object.fromEntries(
        tools.map((tool) => [
          tool.name,
          {
            description: tool.description,
            args: schemaOf(tool).properties ?? {},
            async execute(args, ctx) {
              // No deadline: Kilo puts none on a plugin tool, and the call
              // may wait on a person for hours.
              const answer = await callTool(url, tool.name, args, ctx.abort);
              if (answer.isError) throw new Error(answer.text);
              return answer.text;
            },
          },
        ]),
      ),
      async "tool.definition"(input, output) {
        const tool = byName.get(input.toolID);
        if (tool !== undefined) output.jsonSchema = schemaOf(tool);
      },
    };
  };
}
