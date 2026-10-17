// The decision behind `.opencode/plugins/specialist-commands.ts`: whether one
// shell command a specialist asked to run is on that specialist's own list,
// kept in `specialist_commands.json` beside this file.
//
// A key per agent name (the file name of its `.opencode/agents/` file), and
// under it the commands that agent may run, each as the words a command
// starts with ("weft events" allows `weft events <run id> --node x`). Every
// command in the line (split on &&, ||, ;, |, |& and &, outside quotes) must
// start with one of them. Refused for every specialist, whatever its list:
//   - a command built at run time (`$(...)`, `${...}`, backticks) or written
//     over several lines;
//   - an output redirection (`>`, `>>`, `2>&1`): output is read whole;
//   - an input redirection from outside the project (`< /etc/x`, `< ../x`);
//   - `--on` or `--dispatcher` on a weft command, unless the agent's entry
//     has `"names_targets": true` (only the deployer acts on a cloud install);
//   - a `cd` out of the project.
//
// An entry may be an object instead of a string:
//   {"prefix": "weft test-node", "refuse": [["--tier", "live"], ["--yes"]]}
// refuses the command when any of those word sequences appears in it,
//   {"prefix": "kill", "args": "pids"}
// allows only plain process ids after the prefix, and
//   {"prefix": "... opencode run --agent trial --title trial", "check": "opencode-trial"}
// is the prompt-engineer's trial (see `trialRefusal`). A prefix whose last
// word ends in `@` matches any version (`pnpm dlx sv@` allows
// `pnpm dlx sv@0.17.0 create front`). A leading `timeout <duration>` is
// stripped first.
//
// This is a port of `tangle/claude-code/.claude/hooks/specialist_commands.py`;
// `tokenize` follows Python's `shlex.shlex(posix=True, punctuation_chars=True)`
// with `whitespace_split = True`, which that hook uses.

import { existsSync, readFileSync, realpathSync } from "node:fs"
import { dirname, join, resolve, sep } from "node:path"

export type Entry = string | { prefix: string; refuse?: string[][]; args?: "pids"; check?: "opencode-trial" }
export type AgentList = { names_targets?: boolean; commands: Entry[] }
export type Lists = Record<string, AgentList>

const SEPARATORS = new Set(["&&", "||", ";", "|", "&", "|&"])
const TARGET_FLAGS = ["--on", "--dispatcher"]
const TIMEOUT_DURATION = /^\d+(\.\d+)?[smhd]?$/

export const LISTS_PATH = join(dirname(new URL(import.meta.url).pathname), "specialist_commands.json")

export function readLists(path: string = LISTS_PATH): Lists {
  return JSON.parse(readFileSync(path, "utf-8"))
}

export function prefixOf(item: Entry): string {
  return typeof item === "string" ? item : item.prefix
}

// Why `command` is refused for this entry, or null when it is allowed.
export function refusal(command: string, entry: AgentList, cwd: string, project: string): string | null {
  if (command.includes("\n") || command.includes("\r")) {
    return "a command on several lines is never run: one command per call"
  }
  if (command.includes("$(") || command.includes("${") || command.includes("`")) {
    return "a command built at run time ($(...), ${...} or backticks) is never run"
  }
  let tokens: string[]
  try {
    tokens = tokenize(command)
  } catch (error) {
    return `the command could not be read (${(error as Error).message})`
  }
  const parts: string[][] = []
  let current: string[] = []
  for (const token of tokens) {
    if (SEPARATORS.has(token)) {
      parts.push(current)
      current = []
    } else {
      current.push(token)
    }
  }
  parts.push(current)
  for (const part of parts) {
    if (part.length === 0) return "the command has an empty part"
    const reason = partRefusal(part, entry, cwd, project)
    if (reason !== null) return reason
  }
  return null
}

function partRefusal(input: string[], entry: AgentList, cwd: string, project: string): string | null {
  let words = input
  if (words[0] === "timeout" && words.length > 2 && TIMEOUT_DURATION.test(words[1])) words = words.slice(2)
  const cleaned: string[] = []
  for (let i = 0; i < words.length; ) {
    const word = words[i]
    if (
      word.startsWith(">") ||
      word === ">&" ||
      word === "&>" ||
      (/^\d+$/.test(word) && i + 1 < words.length && words[i + 1].startsWith(">"))
    ) {
      return "an output redirection is never run: read the output whole"
    }
    if (word === "<") {
      if (i + 1 >= words.length || !inside(words[i + 1], cwd, project)) {
        return "an input redirection must read a file inside the project"
      }
      i += 2
      continue
    }
    cleaned.push(word)
    i += 1
  }
  words = cleaned
  if (words.length === 0) return "the command is empty"

  if (words[0] === "weft" && !entry.names_targets) {
    for (const word of words) {
      if (TARGET_FLAGS.includes(word) || TARGET_FLAGS.some((flag) => word.startsWith(flag + "="))) {
        return `\`${word}\` acts on another install, which only the deployer does`
      }
    }
  }

  for (const item of entry.commands ?? []) {
    const prefix = tokenize(prefixOf(item))
    if (!startsWith(words, prefix)) continue
    const rest = words.slice(prefix.length)
    if (prefix.length === 1 && prefix[0] === "cd") {
      if (rest.length === 1 && inside(rest[0], cwd, project)) return null
      return "`cd` goes only to a folder inside the project"
    }
    if (typeof item !== "string") {
      if (item.args === "pids" && !(rest.length > 0 && rest.every((word) => /^\d+$/.test(word) && Number(word) > 1))) {
        return `\`${prefixOf(item)}\` takes only process ids`
      }
      for (const banned of item.refuse ?? []) {
        if (contains(rest, banned)) return `\`${banned.join(" ")}\` is refused for \`${prefixOf(item)}\``
      }
      if (item.check === "opencode-trial") return trialRefusal(rest)
    }
    return null
  }
  return `\`${words.slice(0, 3).join(" ")}\` is not on your list`
}

// The prompt-engineer's trial runs a second OpenCode in a folder outside the
// project, on an agent the prompt-engineer itself wrote into that folder's
// `opencode.json`. An agent written there with tools would run them there,
// past this plugin and past every rule of this project, so the trial runs
// only when that agent can do nothing: the command carries `--dir` (a
// `/tmp/weft-trial-` folder) and `--model`, nothing else, and the folder's
// `opencode.json` holds one agent, `trial`, whose prompt is a `{file:...}`
// and whose permission is `{"*": "deny"}`.
function trialRefusal(rest: string[]): string | null {
  const usage = "the trial is `... --agent trial --title trial --dir /tmp/weft-trial-<name> --model <provider/model>` and nothing more"
  const flags = new Map<string, string>()
  for (let i = 0; i < rest.length; i += 2) {
    const flag = rest[i]
    if ((flag !== "--dir" && flag !== "--model") || flags.has(flag) || i + 1 >= rest.length) return usage
    flags.set(flag, rest[i + 1])
  }
  const dir = flags.get("--dir")
  if (!dir || !flags.has("--model")) return usage
  if (!dir.startsWith("/tmp/weft-trial-") || dir.split("/").includes("..")) {
    return "the trial folder is a `/tmp/weft-trial-<name>` folder"
  }
  for (const other of [".opencode", "AGENTS.md", "CLAUDE.md", "opencode.jsonc", "config.json"]) {
    if (existsSync(join(dir, other))) return `the trial folder holds \`${other}\`; it holds only \`opencode.json\``
  }
  let config: any
  try {
    config = JSON.parse(readFileSync(join(dir, "opencode.json"), "utf-8"))
  } catch (error) {
    return `the trial folder's opencode.json could not be read (${(error as Error).message})`
  }
  const keys = Object.keys(config ?? {}).filter((key) => key !== "$schema")
  const agents = config?.agent
  const trial = agents?.trial
  const ok =
    keys.length === 1 &&
    keys[0] === "agent" &&
    agents &&
    Object.keys(agents).length === 1 &&
    trial &&
    Object.keys(trial).every((key) => ["mode", "prompt", "permission"].includes(key)) &&
    trial.mode === "primary" &&
    typeof trial.prompt === "string" &&
    /^\{file:[^}]+\}$/.test(trial.prompt) &&
    JSON.stringify(trial.permission) === JSON.stringify({ "*": "deny" })
  if (!ok) {
    return 'the trial folder\'s opencode.json holds one agent, `trial`, with only `mode: "primary"`, a `{file:...}` prompt and `permission: {"*": "deny"}`'
  }
  return null
}

function startsWith(words: string[], prefix: string[]): boolean {
  if (words.length < prefix.length) return false
  return prefix.every((want, i) => {
    const last = i === prefix.length - 1
    if (last && want.endsWith("@")) return words[i].startsWith(want)
    return words[i] === want
  })
}

function contains(words: string[], sequence: string[]): boolean {
  const n = sequence.length
  for (let i = 0; i + n <= words.length; i++) {
    if (sequence.every((word, j) => words[i + j] === word)) return true
  }
  return false
}

// Python's os.path.realpath: resolves the symlinks of the part that exists
// and keeps the rest as written.
function realpath(path: string): string {
  let head = resolve(path)
  const tail: string[] = []
  while (!existsSync(head)) {
    const parent = dirname(head)
    if (parent === head) break
    tail.unshift(head.slice(parent.length).replace(/^[\\/]+/, ""))
    head = parent
  }
  const real = existsSync(head) ? realpathSync(head) : head
  return tail.length ? join(real, ...tail) : real
}

// Whether `path`, read from `cwd`, lands inside the project folder.
export function inside(path: string, cwd: string, project: string): boolean {
  if (path.startsWith("~")) return false
  const root = realpath(project)
  const target = realpath(resolve(cwd, path))
  return target === root || target.startsWith(root + sep)
}

const WHITESPACE = " \t\r\n"
const PUNCTUATION = "();<>|&"
const QUOTES = "'\""
const ESCAPE = "\\"
const ESCAPED_QUOTES = '"'
const COMMENTERS = "#"

// shlex.shlex(command, posix=True, punctuation_chars=True) with
// whitespace_split = True, read token by token as `list(lexer)` does.
export function tokenize(text: string): string[] {
  let pos = 0
  const pushback: string[] = []
  let state: string | null = " "
  const next = (): string => {
    if (pushback.length) return pushback.pop() as string
    return pos < text.length ? text[pos++] : ""
  }
  const skipLine = () => {
    while (pos < text.length && text[pos] !== "\n") pos++
    if (pos < text.length) pos++
  }
  const readToken = (): string | null => {
    let quoted = false
    let escapedState = " "
    let token = ""
    while (true) {
      const ch = next()
      if (state === null) {
        token = ""
        break
      } else if (state === " ") {
        if (!ch) {
          state = null
          break
        } else if (WHITESPACE.includes(ch)) {
          if (token || quoted) break
          continue
        } else if (COMMENTERS.includes(ch)) {
          skipLine()
        } else if (ESCAPE.includes(ch)) {
          escapedState = "a"
          state = ch
        } else if (PUNCTUATION.includes(ch)) {
          token = ch
          state = "c"
        } else if (QUOTES.includes(ch)) {
          state = ch
        } else {
          token = ch
          state = "a"
        }
      } else if (QUOTES.includes(state)) {
        quoted = true
        if (!ch) throw new Error("No closing quotation")
        if (ch === state) {
          state = "a"
        } else if (ESCAPE.includes(ch) && ESCAPED_QUOTES.includes(state)) {
          escapedState = state
          state = ch
        } else {
          token += ch
        }
      } else if (ESCAPE.includes(state)) {
        if (!ch) throw new Error("No escaped character")
        if (QUOTES.includes(escapedState) && ch !== state && ch !== escapedState) token += state
        token += ch
        state = escapedState
      } else if (state === "a" || state === "c") {
        if (!ch) {
          state = null
          break
        } else if (WHITESPACE.includes(ch)) {
          state = " "
          if (token || quoted) break
          continue
        } else if (COMMENTERS.includes(ch)) {
          skipLine()
          state = " "
          if (token || quoted) break
          continue
        } else if (state === "c") {
          if (PUNCTUATION.includes(ch)) {
            token += ch
          } else {
            if (!WHITESPACE.includes(ch)) pushback.push(ch)
            state = " "
            break
          }
        } else if (QUOTES.includes(ch)) {
          state = ch
        } else if (ESCAPE.includes(ch)) {
          escapedState = "a"
          state = ch
        } else if (!PUNCTUATION.includes(ch)) {
          token += ch
        } else {
          pushback.push(ch)
          state = " "
          if (token || quoted) break
          continue
        }
      }
    }
    if (!quoted && token === "") return null
    return token
  }
  const tokens: string[] = []
  while (true) {
    const token = readToken()
    if (token === null) break
    tokens.push(token)
  }
  return tokens
}
