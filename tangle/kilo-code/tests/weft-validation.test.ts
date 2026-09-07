import assert from "node:assert/strict"
import test from "node:test"

type ProcessResult = { exit: number; stdout: string; stderr: string }

const files = new Map<string, string>()
const spawns: Array<{ command: string[]; options: any }> = []
const results: ProcessResult[] = []

globalThis.Bun = {
  which: (command: string) => command === "weft" ? "/test/bin/weft" : null,
  file: (path: string) => ({
    exists: async () => files.has(path),
    arrayBuffer: async () => new TextEncoder().encode(files.get(path) ?? "").buffer,
  }),
  spawn: (command: string[], options: any) => {
    spawns.push({ command, options })
    const result = results.shift() ?? { exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" }
    return {
      exited: Promise.resolve(result.exit),
      stdout: new TextEncoder().encode(result.stdout),
      stderr: new TextEncoder().encode(result.stderr),
      kill: () => {},
    }
  },
}

const { default: plugin } = await import("../.kilo/plugin/weft-validation.ts")

const reset = () => {
  files.clear()
  spawns.length = 0
  results.length = 0
}

const run = async (args: any, metadata: any = {}) => {
  const hooks = await plugin({ directory: "/workspace" })
  const output = { output: "tool result", metadata }
  await hooks["tool.execute.after"]({ tool: "apply_patch", args }, output)
  return output
}

test("validates every applicable apply_patch metadata file from its nearest project", async () => {
  reset()
  files.set("/workspace/project/weft.toml", "")
  files.set("/workspace/project/flows/other.weft", "flow")
  files.set("/workspace/project/main.weft", "main")
  results.push({ exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" })
  results.push({ exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" })

  await run({}, { files: [{ filePath: "/workspace/project/flows/other.weft" }, { filePath: "project/nodes/foo/mod.rs" }] })

  assert.deepEqual(spawns.map(({ command }) => command.at(-1)), [
    "/workspace/project/flows/other.weft",
    "/workspace/project/main.weft",
  ])
  assert.equal(spawns[1].options.cwd, "/workspace/project")
  assert.ok(spawns.every(({ options }) => options.stdin instanceof Uint8Array))
})

test("parses every apply_patch header when metadata files are unavailable", async () => {
  reset()
  files.set("/workspace/project/weft.toml", "")
  files.set("/workspace/project/main.weft", "main")
  files.set("/workspace/project/other.weft", "other")
  results.push({ exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" })
  results.push({ exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" })

  await run({ patchText: "*** Begin Patch\n*** Update File: project/main.weft\n*** Add File: project/other.weft\n*** End Patch" })

  assert.deepEqual(spawns.map(({ command }) => command.at(-1)), [
    "/workspace/project/main.weft",
    "/workspace/project/other.weft",
  ])
})

test("reports stderr failures and keeps runtime findings out of edit feedback", async () => {
  reset()
  files.set("/workspace/project/weft.toml", "")
  files.set("/workspace/project/main.weft", "main")
  results.push({ exit: 1, stdout: "", stderr: "catalog unavailable" })
  let output = await run({}, { files: [{ filePath: "project/main.weft" }] })
  assert.match(output.output, /weft validate could not run:\ncatalog unavailable/)

  results.push({
    exit: 0,
    stdout: JSON.stringify({ diagnostics: [
      { severity: "error", code: "rule-runtime", message: "choose a connection" },
      { severity: "warning", code: "level-too-large", line: 4, column: 2, message: "group this level" },
    ] }),
    stderr: "",
  })
  output = await run({}, { files: [{ filePath: "project/main.weft" }] })
  assert.match(output.output, /level-too-large/)
  assert.doesNotMatch(output.output, /rule-runtime/)
})
