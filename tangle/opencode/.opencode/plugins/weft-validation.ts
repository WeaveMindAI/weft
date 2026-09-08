import { dirname, isAbsolute, join, resolve } from "node:path"

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

const readProcessText = async (stream: ReadableStream | Uint8Array | null | undefined) =>
  stream ? new TextDecoder().decode(await new Response(stream).arrayBuffer()) : ""

const validate = async (root: string, target: string) => {
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
    const findings = diagnostics.filter((diagnostic: any) =>
      (diagnostic.severity === "error" && diagnostic.code !== "rule-runtime") ||
      (diagnostic.severity === "warning" && diagnostic.code === "level-too-large"),
    )
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

    const targets = new Set<string>()
    for (const path of changedPaths(input, output)) {
      if (!isWeftSource(path) && !isNodeSource(path)) continue
      const edited = absolutePath(directory, path)
      const root = await projectRoot(edited)
      if (!root) continue
      const target = isWeftSource(path) ? edited : join(root, "main.weft")
      if (await Bun.file(target).exists()) targets.add(target)
    }

    for (const target of targets) {
      const feedback = await validate(await projectRoot(target) ?? directory, target)
      if (feedback) output.output += `\n${feedback}`
    }
  },
})

// OpenCode's documented examples all use a NAMED export, while the sibling
// Kilo plugin (same hook, same signature) is loaded as a default export.
// Exporting both ways costs nothing and means the loader finds it either way.
export const WeftValidationPlugin = plugin

export default plugin
