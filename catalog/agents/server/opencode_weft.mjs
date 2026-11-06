// weft's plugin for OpenCode 2: the program's tools, each under its plain
// name and on the model's own tool list. Its loader (`js_plugin.py`) is the
// `index.mjs` of a plugin folder named in `OPENCODE_CONFIG_CONTENT`'s
// `plugins` list.
//
// An MCP server's tools are `<server>_<tool>`, and by default reached only
// from inside the `execute` tool's code (code mode). A tool a plugin adds
// with no namespace and `codemode: false` is a direct tool under its own
// name; tools are kept by name, and a user plugin's transform runs after
// the built-in tools', so a program tool named `read` takes the place of
// OpenCode's own `read`.

import { callTool, schemaOf } from "./weft_tools.mjs";

export function make({ url, tools }) {
  return {
    id: "weft",
    async setup(ctx) {
      await ctx.tool.transform((editor) => {
        for (const tool of tools) {
          editor.add({
            name: tool.name,
            description: tool.description,
            input: schemaOf(tool),
            options: { codemode: false },
            // No deadline: OpenCode puts none on a plugin tool, and the
            // call may wait on a person for hours.
            async execute(input) {
              const answer = await callTool(url, tool.name, input);
              if (answer.isError) throw new Error(answer.text);
              return { content: answer.text };
            },
          });
        }
      });
    },
  };
}
