import assert from "node:assert/strict"
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
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
  files.set("/workspace/project/src/main.weft", "main")
  results.push({ exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" })
  results.push({ exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" })

  await run({}, { files: [{ filePath: "/workspace/project/flows/other.weft" }, { filePath: "project/nodes/foo/mod.rs" }] })

  assert.deepEqual(spawns.map(({ command }) => command.at(-1)), [
    "/workspace/project/flows/other.weft",
    "/workspace/project/src/main.weft",
  ])
  assert.equal(spawns[1].options.cwd, "/workspace/project")
  assert.ok(spawns.every(({ options }) => options.stdin instanceof Uint8Array))
})

test("parses every apply_patch header when metadata files are unavailable", async () => {
  reset()
  files.set("/workspace/project/weft.toml", "")
  files.set("/workspace/project/src/main.weft", "main")
  files.set("/workspace/project/other.weft", "other")
  results.push({ exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" })
  results.push({ exit: 0, stdout: "{\"diagnostics\":[]}", stderr: "" })

  await run({ patchText: "*** Begin Patch\n*** Update File: project/src/main.weft\n*** Add File: project/other.weft\n*** End Patch" })

  assert.deepEqual(spawns.map(({ command }) => command.at(-1)), [
    "/workspace/project/src/main.weft",
    "/workspace/project/other.weft",
  ])
})

test("an edit to a file main.weft includes, however deep, validates main.weft", async () => {
  reset()
  files.set("/workspace/project/weft.toml", "")
  files.set("/workspace/project/src/main.weft", "one = @include(\"billing/one.weft\")\n# old = @include(\"unused.weft\")\n")
  files.set("/workspace/project/src/billing/one.weft", "two = @include(\"two.weft\")\n")
  files.set("/workspace/project/src/billing/two.weft", "leaf")
  files.set("/workspace/project/src/unused.weft", "commented out")

  await run({}, { files: [{ filePath: "project/src/billing/two.weft" }] })
  await run({}, { files: [{ filePath: "project/src/unused.weft" }] })

  assert.deepEqual(spawns.map(({ command }) => command.at(-1)), [
    "/workspace/project/src/main.weft",
    "/workspace/project/src/unused.weft",
  ])
})

test("reports stderr failures and keeps runtime findings out of edit feedback", async () => {
  reset()
  files.set("/workspace/project/weft.toml", "")
  files.set("/workspace/project/src/main.weft", "main")
  results.push({ exit: 1, stdout: "", stderr: "catalog unavailable" })
  let output = await run({}, { files: [{ filePath: "project/src/main.weft" }] })
  assert.match(output.output, /weft validate could not run:\ncatalog unavailable/)

  results.push({
    exit: 0,
    stdout: JSON.stringify({ diagnostics: [
      { severity: "error", code: "rule-runtime", message: "choose a connection" },
      { severity: "warning", code: "level-too-large", line: 4, column: 2, message: "group this level" },
    ] }),
    stderr: "",
  })
  output = await run({}, { files: [{ filePath: "project/src/main.weft" }] })
  assert.match(output.output, /level-too-large/)
  assert.doesNotMatch(output.output, /rule-runtime/)
})

// A project on disk (the plugin lists `nodes/` and the `.weft` sources
// itself), mirrored into the Bun.file stand-in the plugin reads through.
const diskProject = (disk: Record<string, string>) => {
  const root = mkdtempSync(join(tmpdir(), "weft-hook-"))
  for (const [path, text] of Object.entries(disk)) {
    mkdirSync(join(root, path, ".."), { recursive: true })
    writeFileSync(join(root, path), text)
    files.set(join(root, path), text)
  }
  return root
}

const unwrittenFindings = JSON.stringify({ diagnostics: [
  { severity: "error", code: "enrich", line: 2, column: 0, message: "unknown node type: 'NotYet'" },
  { severity: "error", code: "unknown-target-port", line: 3, column: 2, message: "node 'later' has no input port 'data'. Available: []" },
  { severity: "error", code: "unknown-source-port", line: 5, column: 14, message: "node 'later' has no output port 'value'. Available: []" },
  { severity: "error", code: "unknown-source-port", line: 6, column: 14, message: "node 'greeting' has no output port 'nope'. Available: [value]" },
] })

const mainWeft = "greeting = MyThing { value: \"Hello\" }\nlater = NotYet {\n  data: greeting.value\n}\nout = Debug { data: later.value }\nbad = Debug { data: greeting.nope }\n"

test("an edit under nodes/ leaves out node types no folder declares yet, and what follows from them", async () => {
  reset()
  const root = diskProject({
    "weft.toml": "",
    "src/main.weft": mainWeft,
    "nodes/my_thing/metadata.json": "{ \"type\": \"MyThing\"",
  })
  try {
    results.push({ exit: 0, stdout: unwrittenFindings, stderr: "" })
    const output = await run({}, { files: [{ filePath: join(root, "nodes/my_thing/mod.rs") }] })
    assert.match(output.output, /greeting' has no output port 'nope'/)
    assert.doesNotMatch(output.output, /NotYet|'later'/)
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
})

test("an unknown type some folder declares is still reported on a node edit", async () => {
  reset()
  const root = diskProject({
    "weft.toml": "",
    "src/main.weft": mainWeft,
    "nodes/my_thing/metadata.json": "{ \"type\": \"MyThing\" }",
    "nodes/not_yet/metadata.json": "{ \"type\": \"NotYet\" }",
  })
  try {
    results.push({ exit: 0, stdout: unwrittenFindings, stderr: "" })
    const output = await run({}, { files: [{ filePath: join(root, "nodes/my_thing/mod.rs") }] })
    assert.match(output.output, /unknown node type: 'NotYet'/)
    assert.match(output.output, /'later' has no input port/)
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
})

test("an edit to a .weft file reports unknown node types as before", async () => {
  reset()
  const root = diskProject({ "weft.toml": "", "src/main.weft": mainWeft })
  try {
    results.push({ exit: 0, stdout: unwrittenFindings, stderr: "" })
    const output = await run({}, { files: [{ filePath: join(root, "src/main.weft") }] })
    assert.match(output.output, /unknown node type: 'NotYet'/)
    assert.match(output.output, /'later' has no output port/)
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
})
