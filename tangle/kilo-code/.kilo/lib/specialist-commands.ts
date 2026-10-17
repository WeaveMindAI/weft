// Each specialist runs its own shell commands and no others: the decision.
//
// The lists live in `.kilo/specialist_commands.json`, a key per agent name
// (the file name under `.kilo/agent/`), and under it the commands that agent
// may run, each as the words a command starts with ("weft events" allows
// `weft events <run id> --node x`). `.kilo/plugin/specialist-commands.ts`
// calls `refusal` before every shell command a specialist runs.
//
// The shapes refused for every specialist, whatever its list says:
//   - a command built at run time (`$(...)`, `${...}`, backticks) or written
//     over several lines;
//   - an output redirection (`>`, `>>`, `2>&1`): a command's output is read
//     whole, never sent somewhere else;
//   - an input redirection from outside the project (`< /etc/x`, `< ../x`);
//   - `--on` or `--dispatcher` on a weft command, unless the agent's entry has
//     `"names_targets": true` (only the deployer acts on a cloud install);
//   - a `cd` out of the project.
//
// An entry may be an object instead of a string:
//   {"prefix": "weft test-node", "refuse": [["--tier", "live"], ["--yes"]]}
// refuses the command when any of those word sequences appears in it, and
//   {"prefix": "kill", "args": "pids"}
// allows only plain process ids after the prefix. A prefix whose last word ends
// in `@` matches any version (`pnpm dlx sv@` allows `pnpm dlx sv@0.17.0 create
// front`). A leading `timeout <duration>` is stripped first.
//
// The line is split with a tokenizer that follows Python's
// `shlex.shlex(posix=True, punctuation_chars=True)` with `whitespace_split`,
// the one tangle/claude-code's hook uses, so both read a line the same way
// (operators inside quotes do not split it).
//
// `approved` holds the call ids of the shell commands this decision let
// through, so `.kilo/plugin/refuse-specialist-prompts.ts` can answer the
// permission prompt Kilo raises for one of them with "once" and refuse every
// other prompt a specialist raises.

import { existsSync, readFileSync, realpathSync } from "node:fs"
import { dirname, join, resolve, sep } from "node:path"

export type Item = string | { prefix: string; refuse?: string[][]; args?: "pids" }
export type Entry = { names_targets?: boolean; commands?: Item[] }
export type Lists = Record<string, Entry>

export const LISTS = join(dirname(import.meta.dirname), "specialist_commands.json")

export const approved = new Set<string>()

const SEPARATORS = new Set(["&&", "||", ";", "|", "&", "|&"])
const TARGET_FLAGS = ["--on", "--dispatcher"]
const TIMEOUT_DURATION = /^\d+(\.\d+)?[smhd]?$/
const DIGITS = /^\d+$/

export const loadLists = (path: string = LISTS): Lists => JSON.parse(readFileSync(path, "utf8"))

const WHITESPACE = " \t\r\n"
const PUNCTUATION = "();<>|&"
const QUOTES = "'\""
const ESCAPE = "\\"
const ESCAPED_QUOTES = '"'
const COMMENTERS = "#"

/** The words of `line`, as Python's posix shlex with punctuation_chars and whitespace_split reads them. */
export function tokenize(line: string): string[] {
  const out: string[] = []
  let pos = 0
  let state: string | null = " "
  let token = ""
  const pushback: string[] = []
  const read = () => (pushback.length ? (pushback.pop() as string) : pos < line.length ? line[pos++] : "")
  const skipLine = () => {
    while (pos < line.length && line[pos] !== "\n") pos++
    if (pos < line.length) pos++
  }

  const readToken = (): string | null => {
    let quoted = false
    let escapedState = " "
    while (true) {
      const next = read()
      if (state === null) {
        token = ""
        break
      } else if (state === " ") {
        if (!next) {
          state = null
          break
        } else if (WHITESPACE.includes(next)) {
          if (token || quoted) break
          continue
        } else if (COMMENTERS.includes(next)) {
          skipLine()
        } else if (ESCAPE.includes(next)) {
          escapedState = "a"
          state = next
        } else if (PUNCTUATION.includes(next)) {
          token = next
          state = "c"
        } else if (QUOTES.includes(next)) {
          state = next
        } else {
          token = next
          state = "a"
        }
      } else if (QUOTES.includes(state)) {
        quoted = true
        if (!next) throw new Error("No closing quotation")
        if (next === state) {
          state = "a"
        } else if (ESCAPE.includes(next) && ESCAPED_QUOTES.includes(state)) {
          escapedState = state
          state = next
        } else {
          token += next
        }
      } else if (ESCAPE.includes(state)) {
        if (!next) throw new Error("No escaped character")
        if (QUOTES.includes(escapedState) && next !== state && next !== escapedState) token += state
        token += next
        state = escapedState
      } else {
        // state 'a' (a word) or 'c' (a run of punctuation)
        if (!next) {
          state = null
          break
        } else if (WHITESPACE.includes(next)) {
          state = " "
          if (token || quoted) break
          continue
        } else if (COMMENTERS.includes(next)) {
          skipLine()
          state = " "
          if (token || quoted) break
          continue
        } else if (state === "c") {
          if (PUNCTUATION.includes(next)) {
            token += next
          } else {
            if (!WHITESPACE.includes(next)) pushback.push(next)
            state = " "
            break
          }
        } else if (QUOTES.includes(next)) {
          state = next
        } else if (ESCAPE.includes(next)) {
          escapedState = "a"
          state = next
        } else if (!PUNCTUATION.includes(next)) {
          token += next
        } else {
          pushback.push(next)
          state = " "
          if (token || quoted) break
          continue
        }
      }
    }
    const result = token
    token = ""
    if (!quoted && result === "") return null
    return result
  }

  while (true) {
    const word = readToken()
    if (word === null) break
    out.push(word)
  }
  return out
}

export const prefixOf = (item: Item) => (typeof item === "string" ? item : item.prefix)

/** Why `command` is refused for this entry, or null when it is allowed. */
export function refusal(command: string, entry: Entry, cwd: string, project: string): string | null {
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
    if (!part.length) return "the command has an empty part"
    const reason = partRefusal(part, entry, cwd, project)
    if (reason !== null) return reason
  }
  return null
}

function partRefusal(input: string[], entry: Entry, cwd: string, project: string): string | null {
  let words = input
  if (words[0] === "timeout" && words.length > 2 && TIMEOUT_DURATION.test(words[1])) words = words.slice(2)
  const cleaned: string[] = []
  let i = 0
  while (i < words.length) {
    const word = words[i]
    if (
      word.startsWith(">") ||
      word === ">&" ||
      word === "&>" ||
      (DIGITS.test(word) && i + 1 < words.length && words[i + 1].startsWith(">"))
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
  if (!words.length) return "the command is empty"

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
      if (item.args === "pids" && !(rest.length && rest.every((word) => DIGITS.test(word) && Number(word) > 1))) {
        return `\`${prefixOf(item)}\` takes only process ids`
      }
      for (const banned of item.refuse ?? []) {
        if (contains(rest, banned)) return `\`${banned.join(" ")}\` is refused for \`${prefixOf(item)}\``
      }
    }
    return null
  }
  return `\`${words.slice(0, 3).join(" ")}\` is not on your list`
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

/** A path with every link resolved, like Python's os.path.realpath: the part that exists is resolved, the rest appended. */
function realpath(path: string): string {
  const absolute = resolve(path)
  let head = absolute
  const tail: string[] = []
  while (!existsSync(head)) {
    const parent = dirname(head)
    if (parent === head) return absolute
    tail.unshift(head.slice(parent.length).replace(/^[\\/]/, ""))
    head = parent
  }
  return tail.length ? join(realpathSync(head), ...tail) : realpathSync(head)
}

/** Whether `path`, read from `cwd`, lands inside the project folder. */
export function inside(path: string, cwd: string, project: string): boolean {
  if (path.startsWith("~")) return false
  const root = realpath(project)
  const target = realpath(resolve(cwd, path))
  return target === root || target.startsWith(root + sep)
}

/** The message a refused command comes back with. */
export function refusalMessage(agent: string, reason: string, entry: Entry): string {
  const allowed = (entry.commands ?? []).map(prefixOf).join(", ")
  return `${reason}. As ${agent} you may run only: ${allowed}. Do it another way, or report BLOCKED with the exact command.`
}
