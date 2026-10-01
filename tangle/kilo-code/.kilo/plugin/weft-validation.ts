import { readdir, readFile } from "node:fs/promises"
import { basename, dirname, isAbsolute, join, resolve } from "node:path"

const VALIDATE_TIMEOUT_MS = 5_000

const isWeftSource = (path: unknown) => typeof path === "string" && path.endsWith(".weft")
const isNodeSource = (path: unknown) =>
  typeof path === "string" && path.split(/[\\/]+/).includes("nodes")

const absolutePath = (directory: string, path: string) => isAbsolute(path) ? path : resolve(directory, path)

const patchPaths = (patchText: unknown) => {
  if (typeof patchText !== "string") return []
  return [...patchText.matchAll(/^\*\*\* (?:Add|Update|Delete) File: (.+)$/gm)].map((match) => match[1])
}

const changedPaths = (input: { args?: any }, output: { metadata?: any }) => {
  const files = output.metadata?.files
  const metadataPaths = Array.isArray(files)
    ? files.flatMap((file) => {
        if (typeof file === "string") return [file]
        if (!file || typeof file !== "object") return []
        return [file.filePath, file.file_path, file.movePath, file.move_path].filter(
          (path): path is string => typeof path === "string",
        )
      })
    : []
  return [
    ...metadataPaths,
    output.metadata?.filePath,
    output.metadata?.file_path,
    input.args?.filePath,
    input.args?.file_path,
    ...patchPaths(output.metadata?.patchText),
    ...patchPaths(input.args?.patchText),
  ].filter((path): path is string => typeof path === "string")
}

const projectRoot = async (start: string) => {
  let current = dirname(start)
  while (true) {
    if (await Bun.file(join(current, "weft.toml")).exists()) return current
    const parent = dirname(current)
    if (parent === current) return
    current = parent
  }
}

const INCLUDE = /@include\(\s*"([^"]+)"\s*\)/g

// The file to validate for an edit to a `.weft` file: the project's entry
// `src/main.weft` when the file is that entry or is pulled in by it, directly
// or through other includes (an included file checked alone misses what its
// includer wires into it, and reports false errors such as an unmet required
// port); the file itself when nothing includes it. An `@include` path is
// relative to the file that writes it.
const entryFor = async (root: string, edited: string) => {
  const entry = join(root, "src", "main.weft")
  if (!(await Bun.file(entry).exists())) return edited
  const want = resolve(edited)
  const seen = new Set<string>()
  const stack = [resolve(entry)]
  while (stack.length) {
    const current = stack.pop() as string
    if (seen.has(current)) continue
    seen.add(current)
    if (current === want) return entry
    if (!(await Bun.file(current).exists())) continue
    let text: string
    try {
      text = new TextDecoder().decode(await Bun.file(current).arrayBuffer())
    } catch {
      continue
    }
    for (const line of text.split("\n")) {
      if (line.trimStart().startsWith("#")) continue
      for (const match of line.matchAll(INCLUDE)) stack.push(resolve(dirname(current), match[1]))
    }
  }
  return edited
}


// Findings that say the catalog does not serve a node type, with the type.
const UNWRITTEN = [
  /unknown node type:? '([^']+)'/,
  /node type '([^']+)' is not ready yet/,
  /node '([^']+)' failed to load/,
]
const unwrittenType = (diagnostic: any) => {
  for (const pattern of UNWRITTEN) {
    const match = String(diagnostic.message ?? "").match(pattern)
    if (match) return match[1]
  }
}

// Every file under `dir` whose name passes `keep`, with its text.
const filesUnder = async (dir: string, keep: (name: string) => boolean) => {
  let names: string[]
  try {
    names = (await readdir(dir, { recursive: true })) as string[]
  } catch {
    return []
  }
  const out: string[] = []
  for (const name of names) {
    if (!keep(basename(name))) continue
    try {
      out.push(await readFile(join(dir, name), "utf8"))
    } catch {}
  }
  return out
}

const escapeRegExp = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")

// For an edit under `nodes/`: drop the findings about node types no folder
// declares yet. The program uses them, but they are another node's unwritten
// work, not the edit's error. Findings that follow from them go too: any
// finding on the line of such a node, or naming a node declared with such a
// type. Everything else, the edited node's own type and package included, is
// kept. A regex reads each `metadata.json`'s type, so a half-written file
// still names it.
const dropUnwritten = async (root: string, target: string, findings: any[]) => {
  const unwritten = new Set(findings.map(unwrittenType).filter((type): type is string => !!type))
  if (!unwritten.size) return findings
  for (const text of await filesUnder(join(root, "nodes"), (name) => name === "metadata.json")) {
    const match = text.match(/"type"\s*:\s*"([^"]+)"/)
    if (match) unwritten.delete(match[1])
  }
  if (!unwritten.size) return findings
  const names: RegExp[] = []
  for (const text of await filesUnder(dirname(target), (name) => name.endsWith(".weft"))) {
    for (const match of text.matchAll(/^\s*([A-Za-z_]\w*)\s*=\s*([A-Z]\w*)\b/gm)) {
      if (unwritten.has(match[2])) names.push(new RegExp(`['.]${escapeRegExp(match[1])}'`))
    }
  }
  const lines = new Set(
    findings.filter((d) => unwritten.has(unwrittenType(d) ?? "")).map((d) => `${d.file ?? ""}:${d.line}`),
  )
  return findings.filter((d) =>
    !unwritten.has(unwrittenType(d) ?? "") &&
    !lines.has(`${d.file ?? ""}:${d.line}`) &&
    !names.some((name) => name.test(String(d.message ?? ""))),
  )
}

const readProcessText = async (stream: ReadableStream | Uint8Array | null | undefined) =>
  stream ? new TextDecoder().decode(await new Response(stream).arrayBuffer()) : ""

// `nodeEdit`: the edit was under `nodes/`, so findings about node types
// nobody has written yet are left out (`dropUnwritten`). The plugin only
// reports: it never changes, reverts or undoes an edited file.
const validate = async (root: string, target: string, nodeEdit: boolean) => {
  if (!Bun.which("weft")) return

  let source: Uint8Array
  try {
    source = new Uint8Array(await Bun.file(target).arrayBuffer())
  } catch {
    return
  }

  let process: ReturnType<typeof Bun.spawn>
  try {
    process = Bun.spawn(["weft", "validate", "--file", target], {
      cwd: root,
      stdin: source,
      stdout: "pipe",
      stderr: "pipe",
    })
  } catch {
    return
  }

  let timeout: ReturnType<typeof setTimeout> | undefined
  const completed = Promise.all([process.exited, readProcessText(process.stdout), readProcessText(process.stderr)])
  const result = await Promise.race([
    completed,
    new Promise<undefined>((resolve) => {
      timeout = setTimeout(() => {
        process.kill()
        resolve(undefined)
      }, VALIDATE_TIMEOUT_MS)
    }),
  ])
  if (timeout) clearTimeout(timeout)
  if (!result) return

  const [exitCode, stdout, stderr] = result
  if (exitCode !== 0) {
    const text = (stderr || stdout).trim()
    return text ? `weft validate could not run:\n${text}` : undefined
  }

  try {
    const diagnostics = JSON.parse(stdout).diagnostics ?? []
    let findings = diagnostics.filter((diagnostic: any) =>
      (diagnostic.severity === "error" && diagnostic.code !== "rule-runtime") ||
      (diagnostic.severity === "warning" && diagnostic.code === "level-too-large"),
    )
    if (nodeEdit) findings = await dropUnwritten(root, target, findings)
    if (!findings.length) return
    return "weft validation feedback:\n" + findings.map((diagnostic: any) =>
      `  ${diagnostic.file ?? target}:${diagnostic.line ?? "?"}:${diagnostic.column ?? "?"} [${diagnostic.code ?? "diagnostic"}] ${diagnostic.message ?? ""}`,
    ).join("\n")
  } catch {
    return
  }
}

const plugin = async ({ directory }: { directory: string }) => ({
  "tool.execute.after": async (input: { tool: string; args?: any }, output: { output: string; metadata?: any }) => {
    if (!(["edit", "write", "apply_patch"] as string[]).includes(input.tool)) return

    // Each target, and whether every edit that asked for it was under `nodes/`.
    const targets = new Map<string, boolean>()
    for (const path of changedPaths(input, output)) {
      if (!isWeftSource(path) && !isNodeSource(path)) continue
      const edited = absolutePath(directory, path)
      const root = await projectRoot(edited)
      if (!root) continue
      const target = isWeftSource(path) ? await entryFor(root, edited) : join(root, "src", "main.weft")
      if (await Bun.file(target).exists()) targets.set(target, (targets.get(target) ?? true) && !isWeftSource(path))
    }

    for (const [target, nodeEdit] of targets) {
      const feedback = await validate(await projectRoot(target) ?? directory, target, nodeEdit)
      if (feedback) output.output += `\n${feedback}`
    }
  },
})

export default plugin
