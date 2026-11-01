// The decision behind `.opencode/plugins/specialist-commands.ts`: whether one
// shell command a specialist asked to run is on that specialist's own list,
// kept in `specialist_commands.json` beside this file.
//
// A key per agent name (the file name of its `.opencode/agents/` file), and
// under it the commands that agent may run, each as the words a command
// starts with ("weft events" allows `weft events <run-id> --node x`). Every
// command in the line (split on &&, ||, ;, |, |& and &, outside quotes) must
// start with one of them. Refused for every specialist, whatever its list:
//   - a command written over several lines, and any shell syntax the line is
//     not read for (`shellSyntaxRefusal` below): outside quotes, a `$` in any
//     use (`$NAME`, `$(...)`, `${...}`, `$'...'`, `$1`), a backtick, `<(...)`
//     and `>(...)`, an input redirection other than a plain `<`, a `{`, `}`,
//     `(`, `)`, `[` or `#`, and a word starting with `=` (zsh reads those too);
//     inside double quotes, a `$` or a backtick;
//   - an output redirection (`>`, `>>`, `2>&1`): output is read whole;
//   - an input redirection from outside the project (`< /etc/x`, `< ../x`);
//   - `--on` or `--dispatcher` on a weft command, unless the agent's entry
//     has `"names_targets": true` (only the deployer acts on a cloud install);
//   - a `cd` out of the project, or one not written as `cd ./<folder> && ...`
//     (the `./` makes the shell skip CDPATH; the parts after it are checked
//     from that folder), or one whose folder holds a `..` or goes through a
//     link.
//
// An entry may be an object instead of a string:
//   {"prefix": "weft test-node", "refuse": [["--tier", "live"], ["--yes"]]}
// refuses the command when any of those word sequences appears in it,
//   {"prefix": "kill", "args": "pids"}
// allows only plain process ids after the prefix, and
//   {"prefix": "... opencode run --agent trial --title trial", "check": "opencode-trial"}
// is the prompt-engineer's trial (see `trialRefusal`). Every other `"check"`
// runs that tool's own check on the rest of the line (`CHECKS` below), each
// knowing the options it allows and refusing every other: `grep` and `rg`
// search inside the project and never follow a link out of it (`rg` never with
// an option that runs another program, like `--pre`); `ffprobe` reads one file
// inside the project and `ffmpeg` reads files inside it, each as `-f <format>
// -i <file>` so neither guesses a file's format; `ffmpeg` writes only under
// the entry's `"writes_under"` folder; `psql` opens a read-only transaction
// (`-c "BEGIN TRANSACTION READ ONLY"`) and runs one SELECT, SHOW or EXPLAIN
// statement in it. Postgres refuses every write to the database inside that
// transaction, a function the SELECT calls included (`nextval`, say). A
// function whose effect lands outside the transaction (ending another session,
// reading a server file, `dblink`, an advisory lock, `set_config`) is refused
// by its name in the statement (`PSQL_OUTSIDE_EFFECTS`), which cannot see one
// called from inside another function or a view. A prefix whose last word ends
// in `@` matches any version (`pnpm dlx sv@` allows `pnpm dlx sv@0.17.0 create
// front`). A leading `timeout <duration>` is stripped first.
//
// This is a port of `tangle/claude-code/.claude/hooks/specialist_commands.py`;
// `tokenize` follows Python's `shlex.shlex(posix=True, punctuation_chars=True)`
// with `whitespace_split = True`, which that hook uses.

import { existsSync, lstatSync, readdirSync, readFileSync, realpathSync, statSync } from "node:fs"
import { dirname, join, matchesGlob, resolve, sep } from "node:path"

export type Entry = string | { prefix: string; refuse?: string[][]; args?: "pids"; check?: string; writes_under?: string }
type CheckedItem = Exclude<Entry, string>
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
  const syntax = shellSyntaxRefusal(command)
  if (syntax !== null) return syntax
  let tokens: string[]
  try {
    tokens = tokenize(command)
  } catch (error) {
    return `the command could not be read (${(error as Error).message})`
  }
  // Each part with the separator that follows it (null for the last).
  const parts: [string[], string | null][] = []
  let current: string[] = []
  for (const token of tokens) {
    if (SEPARATORS.has(token)) {
      parts.push([current, token])
      current = []
    } else {
      current.push(token)
    }
  }
  parts.push([current, null])

  for (const [part, separator] of parts) {
    if (part.length === 0) return "the command has an empty part"
    const reason = partRefusal(part, entry, cwd, project)
    if (reason !== null) return reason
    // An accepted `cd` moves where the parts after it run, so they are read
    // from there; only `&&` makes sure they run after it, in that folder.
    if (part[0] === "cd") {
      if (separator !== "&&" && separator !== null) return "a `cd` is followed only by `&&`: write `cd ./<folder> && <command>`"
      cwd = realpath(resolve(cwd, part[1]))
    }
  }
  return null
}

// Why `command` holds shell syntax the tokenizer does not read the way bash
// does, or null. The tokenizer knows words, quotes, the separators and the
// redirections, and nothing else, so anything else bash would act on is
// refused here, before a word is read. The walk follows bash's quoting:
// inside '...' everything is literal up to the next `'`; inside "..." a
// backslash escapes only `"` and `\`, and a `$` or a backtick is refused;
// outside quotes a backslash escapes the next character. A `$` that bash
// leaves as it is (at the end of the line, before a space, or before the `"`
// that closes its quotes, as in "route$") passes. The line may run in zsh as
// well (the user's login shell), so what zsh alone acts on is refused too:
// `(` and `)` outside quotes (a glob qualifier like `*(e:...:)` runs code,
// and `=(...)` runs a command), and a word starting with an unquoted `=`
// (`=ls` becomes the path of `ls`).
export function shellSyntaxRefusal(command: string): string | null {
  let quote: string | null = null
  let wordStart = true
  for (let i = 0; i < command.length; i++) {
    const c = command[i]
    const after = i + 1 < command.length ? command[i + 1] : ""
    if (quote === "'") {
      if (c === "'") quote = null
    } else if (quote === '"') {
      if (c === '"') {
        quote = null
      } else if (c === "\\") {
        if (after === "$" || after === "`") {
          return `\`\\${after}\` inside double quotes is read differently by the shell and by this check: put the text in single quotes`
        }
        if (after === '"' || after === "\\") i++
      } else if (c === "`" || (c === "$" && !['"', " ", "\t"].includes(after))) {
        return shellSyntaxMessage(c, after)
      }
    } else if (c === "=" && wordStart) {
      return "a word starting with `=` outside quotes is a path to a program in zsh: put it in quotes"
    } else if (c === "\\") {
      i++
    } else if (c === "'" || c === '"') {
      quote = c
    } else if (c === "[") {
      return "`[` outside quotes is a pattern each shell reads its own way: put the pattern in quotes, or write the path out"
    } else if ("`{}#".includes(c) || (c === "$" && !["", " ", "\t"].includes(after))) {
      return shellSyntaxMessage(c, after)
    } else if ((c === "<" || c === ">") && after === "(") {
      return `\`${c}(...)\` runs a command to make a file: write the file's name out`
    } else if (c === "<" && ["<", ">", "&"].includes(after)) {
      return `\`<${after}\` is never run: the only input redirection here is a plain \`< file\` inside the project`
    } else if (c === "(" || c === ")") {
      return `\`${c}\` outside quotes is shell syntax (a subshell, or in zsh a glob qualifier or \`=(...)\`, which run code): put it in quotes`
    }
    wordStart = quote === null && [" ", "\t", ";", "|", "&", "<", ">"].includes(c)
  }
  return null
}

function shellSyntaxMessage(c: string, after: string): string {
  if (c === "$") {
    return (
      `\`$${after}\` is expanded by the shell before the command runs (a variable, \`$(...)\`, \`\${...}\`, \`$'...'\` and the like), ` +
      "which this check cannot follow: write the value out, or put text meant literally in single quotes"
    )
  }
  if (c === "`") return "a backtick runs a command inside the line: write its output out instead"
  if (c === "#") return "`#` outside quotes is a shell comment, which this check cannot follow: put it in quotes"
  return `\`${c}\` outside quotes is shell syntax (brace expansion or a group): write each word out, or put it in quotes`
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
      const folder = rest[0]
      if (input.length === 2 && rest.length === 1 && folder.startsWith("./") && !GLOB.test(folder) && !folder.split("/").includes("..") && !throughLink(folder, cwd) && isFolder(resolve(cwd, folder)) && inside(folder, cwd, project)) {
        return null
      }
      return "`cd` goes only to a folder inside the project, written out with no `..` and no link on the way: `cd ./<folder>`"
    }
    if (typeof item !== "string") {
      if (item.args === "pids" && !(rest.length > 0 && rest.every((word) => /^\d+$/.test(word) && Number(word) > 1))) {
        return `\`${prefixOf(item)}\` takes only process ids`
      }
      for (const banned of item.refuse ?? []) {
        if (contains(rest, banned)) return `\`${banned.join(" ")}\` is refused for \`${prefixOf(item)}\``
      }
      return checkRefusal(item, rest, cwd, project)
    }
    return null
  }
  return `\`${words.slice(0, 3).join(" ")}\` is not on your list`
}

// The checks an entry names with `"check"`. Each reads the words after the
// entry's prefix and says why they are refused, or null. A tool whose own
// options can run a program, write a file or reach past the project is
// allowed only in the shapes its check knows; any option the check does not
// know is refused, so a new or rare option never slips through.

const GLOB = /[*?[]/
const RG_RUNS = ["--pre", "--pre-glob", "--search-zip", "--hostname-bin"]
const RG_SHORT_TAKES_VALUE = "ABCdEefgjmMrtT"
const GREP_SHORT_TAKES_VALUE = "ABCDdefm"
const FFPROBE_FLAGS = new Set([
  "hide_banner", "show_format", "show_streams", "show_frames", "show_packets",
  "show_chapters", "show_programs", "show_error", "show_data", "show_private_data",
  "count_frames", "count_packets", "pretty", "sexagesimal", "unit", "prefix",
  "byte_binary_prefix", "bitexact",
])
const FFPROBE_VALUES = new Set([
  "v", "loglevel", "of", "print_format", "output_format", "show_entries",
  "select_streams", "read_intervals",
])
const FFMPEG_FLAGS = new Set(["hide_banner", "y", "n", "nostdin", "nostats", "stats", "an", "vn", "sn", "dn", "shortest", "re"])
const FFMPEG_VALUES = new Set([
  "v", "loglevel", "ss", "t", "to", "sseof", "r", "framerate", "s", "frames",
  "vframes", "aframes", "c", "codec", "vcodec", "acodec", "b", "q", "qscale",
  "crf", "preset", "tune", "profile", "level", "pix_fmt", "ar", "ac", "map",
  "loop", "update", "movflags", "aspect", "g", "threads", "metadata",
])
// An input's format is always named, so ffmpeg never guesses it from the
// file: a playlist or a concat list it guessed would read the files it names.
const MEDIA_INPUT_USAGE = "reads an input only as `-f <format> -i <file>`, the format named (`-f mp4 -i clip.mp4`), so it never guesses a file's format"
const FFMPEG_FILTER_OPTIONS = new Set(["vf", "af", "filter"])
// Formats that read and write one plain file (or a numbered run of images):
// `lavfi`, `concat`, `tee`, `segment` and the like reach other files.
const FFMPEG_FORMATS = new Set(["image2", "mp4", "mov", "webm", "matroska", "gif", "apng", "mp3", "wav", "ogg", "flac", "null"])
// Filters that only reshape what flows through them; `movie`, `drawtext`,
// `subtitles`, `sendcmd`, `metadata` and the like read or write files.
const FFMPEG_FILTERS = new Set([
  "scale", "fps", "crop", "pad", "trim", "atrim", "setpts", "asetpts", "select",
  "aselect", "thumbnail", "tile", "format", "aformat", "hflip", "vflip",
  "transpose", "split", "asplit", "palettegen", "paletteuse", "volume",
  "aresample", "loudnorm", "fade", "afade", "reverse", "areverse", "setsar",
  "setdar", "framestep", "null", "anull", "showwavespic", "showspectrumpic",
])
const PSQL_FLAGS = "AtxqXw"
const PSQL_VALUES: Record<string, string> = { h: "host", p: "port", U: "username", d: "dbname", c: "command", F: "field-separator", R: "record-separator" }
const PSQL_LONG_VALUES: Record<string, string> = Object.fromEntries(Object.entries(PSQL_VALUES).map(([short, long]) => [long, short]))
const PSQL_LONG_FLAGS = new Set(["csv", "no-align", "tuples-only", "expanded", "quiet", "no-psqlrc", "no-password"])
const PSQL_READ = /^(select|show|explain)\b/i
// Functions whose effect lands outside the read-only transaction, where
// Postgres does not refuse it: they end or signal other sessions, reach the
// server's files or another database, take a lock or send a message that
// outlives the statement, change the server or the session, or run SQL
// handed to them as text (which could name any of the others).
const PSQL_OUTSIDE_EFFECTS = [
  "dblink\\w*", "pg_terminate_backend", "pg_cancel_backend", "pg_signal_backend",
  "pg_reload_conf", "pg_rotate_logfile\\w*", "pg_log_backend_memory_contexts",
  "lo_export", "lo_import", "pg_read_file", "pg_read_binary_file", "pg_ls_\\w+",
  "pg_stat_file", "pg_file_\\w+", "pg_(?:try_)?advisory_\\w+", "pg_notify",
  "pg_switch_wal", "pg_create_restore_point", "pg_logical_emit_message",
  "pg_backup_\\w+", "pg_promote", "pg_wal_replay_\\w+", "pg_stat_reset\\w*",
  "pg_\\w*replication_slot\\w*", "pg_replication_origin_\\w+", "set_config",
  "query_to_xml\\w*", "cursor_to_xml\\w*", "ts_stat",
]
const PSQL_OUTSIDE_EFFECT = new RegExp(`\\b(${PSQL_OUTSIDE_EFFECTS.join("|")})\\b`, "i")
// Connection parameters that hand libpq settings this check never sees: a
// service file can name a key log, and `options` sets the server's own.
const PSQL_HIDDEN_PARAMETERS = /(?:^|[?&\s])(service|servicefile|options)\s*=/
const PSQL_USAGE = '`psql` runs only as `psql <connection> -c "BEGIN TRANSACTION READ ONLY" -c "<one SELECT, SHOW or EXPLAIN>"`'

type Check = (item: CheckedItem, rest: string[], cwd: string, project: string) => string | null

function checkRefusal(item: CheckedItem, rest: string[], cwd: string, project: string): string | null {
  if (item.check === undefined) return null
  const checker = CHECKS[item.check]
  if (!checker) return `the list names a check \`${item.check}\` this plugin does not know`
  return checker(item, rest, cwd, project)
}

// grep: no option that follows a link out of the project, and every word
// that reads as a path inside it.
const grepRefusal: Check = (_item, rest, cwd, project) => {
  for (const word of rest) {
    const name = word.startsWith("--") ? word.slice(2).split("=")[0] : ""
    // grep takes any unambiguous start of a long option (`--deref`).
    if (name && "dereference-recursive".startsWith(name)) {
      return `\`${word}\` follows links out of the project: search with \`-r\`, which does not`
    }
    if (shortLetters(word, GREP_SHORT_TAKES_VALUE).includes("R")) {
      return `\`${word}\` holds \`-R\`, which follows links out of the project: search with \`-r\`, which does not`
    }
  }
  return pathsRefusal(rest, cwd, project)
}

// rg: no option that runs another program or follows a link out of the
// project, and every path inside it.
const rgRefusal: Check = (_item, rest, cwd, project) => {
  for (const word of rest) {
    if (RG_RUNS.some((flag) => word === flag || word.startsWith(flag + "="))) {
      return `\`${word}\` runs another program: \`rg\` never does here`
    }
    if (word === "--follow") return "`--follow` follows links out of the project: `rg` never does here"
    const letters = shortLetters(word, RG_SHORT_TAKES_VALUE)
    if (letters.includes("z")) return `\`${word}\` holds \`-z\`, which runs a decompressor: \`rg\` never runs another program here`
    if (letters.includes("L")) return `\`${word}\` holds \`-L\`, which follows links out of the project: \`rg\` never does here`
  }
  return pathsRefusal(rest, cwd, project)
}

// Why a search's words reach outside the project, or null.
function pathsRefusal(rest: string[], cwd: string, project: string): string | null {
  for (const word of rest) {
    if (readsOutside(word, cwd, project)) return `\`${word}\` reaches outside the project: search inside it`
  }
  return null
}

// The option letters of a cluster like `-rnL`, up to the first one that
// takes a value (whatever follows it is that value). Empty for any other word.
function shortLetters(word: string, takesValue: string): string {
  if (!word.startsWith("-") || word.startsWith("--")) return ""
  let letters = ""
  for (const letter of word.slice(1)) {
    letters += letter
    if (takesValue.includes(letter)) break
  }
  return letters
}

// ffprobe: known options only, and one input inside the project.
const ffprobeRefusal: Check = (_item, rest, cwd, project) => {
  let inputs = 0
  for (let i = 0; i < rest.length; i++) {
    let word = rest[i]
    const name = word.startsWith("-") && word.length > 1 ? word.slice(1) : null
    if (name !== null && FFPROBE_FLAGS.has(name)) continue
    if (name !== null && FFPROBE_VALUES.has(name) && i + 1 < rest.length) {
      i++
      continue
    }
    if (name === "f" && i + 1 < rest.length) {
      if (!FFMPEG_FORMATS.has(rest[i + 1])) return formatRefusal(rest[i + 1])
      i++
      continue
    }
    if (name === "i" && i + 1 < rest.length) {
      if (!formatNamed(rest, i)) return `\`ffprobe\` ${MEDIA_INPUT_USAGE}`
      i++
      word = rest[i]
    } else if (name !== null) {
      return `\`ffprobe ${word}\` is not an option you may use here`
    } else {
      return `\`ffprobe\` ${MEDIA_INPUT_USAGE}`
    }
    const reason = mediaInputRefusal(word, cwd, project)
    if (reason !== null) return reason
    inputs++
  }
  return inputs === 1 ? null : "`ffprobe` reads exactly one file"
}

// ffmpeg: known options only, inputs inside the project, every output under
// the entry's `writes_under`.
const ffmpegRefusal: Check = (item, rest, cwd, project) => {
  const folder = item.writes_under
  if (typeof folder !== "string" || !folder) return "the list gives `ffmpeg` no `writes_under` folder"
  const under = realpath(join(project, folder))
  let inputs = 0
  let outputs = 0
  let pendingFormat: string | null = null
  for (let i = 0; i < rest.length; i++) {
    const word = rest[i]
    if (word.startsWith("-") && word.length > 1) {
      const name = word.slice(1).split(":")[0]
      if (FFMPEG_FLAGS.has(name)) continue
      if (i + 1 >= rest.length) return `\`ffmpeg ${word}\` needs a value`
      const value = rest[i + 1]
      if (name === "i") {
        if (!formatNamed(rest, i)) return `\`ffmpeg\` ${MEDIA_INPUT_USAGE}`
        const reason = mediaInputRefusal(value, cwd, project)
        if (reason !== null) return reason
        inputs++
        pendingFormat = null
      } else if (name === "f") {
        if (!FFMPEG_FORMATS.has(value)) return formatRefusal(value)
        pendingFormat = value
      } else if (FFMPEG_FILTER_OPTIONS.has(name)) {
        const reason = filtergraphRefusal(value)
        if (reason !== null) return reason
      } else if (!FFMPEG_VALUES.has(name)) {
        return `\`ffmpeg ${word}\` is not an option you may use here`
      }
      i++
      continue
    }
    if (!(word === "-" && pendingFormat === "null")) {
      const target = realpath(resolve(cwd, word))
      if (/[:|]/.test(word) || GLOB.test(word) || !target.startsWith(under + sep)) {
        return `\`ffmpeg\` writes only under \`${folder}/\` in the project, and \`${word}\` is not there`
      }
    }
    outputs++
    pendingFormat = null
  }
  if (!inputs || !outputs) return "`ffmpeg` here takes at least one `-i <input>` and one output file"
  return null
}

// psql: connection options and exactly one read-only `-c` statement.
const psqlRefusal: Check = (_item, rest) => {
  const commands: string[] = []
  let positional = 0
  for (let i = 0; i < rest.length; i++) {
    const word = rest[i]
    if (word.startsWith("--")) {
      const eq = word.indexOf("=")
      const name = eq < 0 ? word.slice(2) : word.slice(2, eq)
      if (eq < 0 && PSQL_LONG_FLAGS.has(name)) continue
      if (!(name in PSQL_LONG_VALUES)) return `\`psql ${word}\` is not an option you may use here. ${PSQL_USAGE}`
      let value = word.slice(eq + 1)
      if (eq < 0) {
        if (i + 1 >= rest.length) return `\`psql ${word}\` needs a value`
        value = rest[++i]
      }
      if (PSQL_LONG_VALUES[name] === "c") commands.push(value)
      else {
        const reason = connectionRefusal(value)
        if (reason !== null) return reason
      }
      continue
    }
    if (word.startsWith("-") && word.length > 1) {
      for (let j = 1; j < word.length; j++) {
        const letter = word[j]
        if (PSQL_FLAGS.includes(letter)) continue
        if (!(letter in PSQL_VALUES)) return `\`psql -${letter}\` is not an option you may use here. ${PSQL_USAGE}`
        let value = word.slice(j + 1)
        if (!value) {
          if (i + 1 >= rest.length) return `\`psql -${letter}\` needs a value`
          value = rest[++i]
        }
        if (letter === "c") commands.push(value)
        else {
          const reason = connectionRefusal(value)
          if (reason !== null) return reason
        }
        break
      }
      continue
    }
    positional++
    if (positional > 2) return `\`${word}\` is one word too many. ${PSQL_USAGE}`
    const reason = connectionRefusal(word)
    if (reason !== null) return reason
  }
  if (commands.length !== 2 || !readOnlyBegin(commands[0])) return PSQL_USAGE
  return sqlRefusal(commands[1])
}

// Why a connection value (a host, a user, a database name or a whole
// connection string) is refused, or null. libpq decodes `%xx` in a URI's
// parameter names as well as its values, so the value is read decoded.
function connectionRefusal(value: string): string | null {
  let decoded = value
  if (value.includes("%")) {
    if (/%(?![0-9A-Fa-f]{2})/.test(value)) {
      return `\`${value}\` holds a \`%\` that is not followed by two hex digits: write the connection out plainly`
    }
    decoded = value.replace(/%([0-9A-Fa-f]{2})/g, (_, hex: string) => String.fromCharCode(parseInt(hex, 16)))
  }
  const lowered = decoded.toLowerCase()
  if (lowered.includes("keylog")) return "a connection that writes a key log is never opened here"
  const hidden = PSQL_HIDDEN_PARAMETERS.exec(lowered)
  if (hidden !== null) {
    return `a connection with \`${hidden[1]}=\` takes settings from somewhere this check cannot read: name the host, port, user and database yourself`
  }
  return null
}

// Whether `sql` opens a read-only transaction, in which Postgres refuses
// every write, and the switch back to read-write once a query has run.
function readOnlyBegin(sql: string): boolean {
  return sql.trim().replace(/;$/, "").trim().split(/\s+/).join(" ").toLowerCase() === "begin transaction read only"
}

// Why one `-c` statement is not a plain read, or null.
function sqlRefusal(sql: string): string | null {
  if (sql.includes("\\")) return `a backslash is never in the statement. ${PSQL_USAGE}`
  let statement = sql.trim()
  if (statement.endsWith(";")) statement = statement.slice(0, -1)
  if (statement.includes(";")) return `the statement is one statement, with no \`;\` inside it. ${PSQL_USAGE}`
  if (/u&['"]|\buescape\b/i.test(statement)) {
    return 'a Unicode-escaped name or string (`U&"..."`, `UESCAPE`) is never in the statement: write names plainly'
  }
  const head = PSQL_READ.exec(statement)
  if (!head) return PSQL_USAGE
  if (/\binto\b/i.test(statement)) return "a `SELECT ... INTO` makes a table, which a read never does"
  if (head[1].toLowerCase() === "explain" && /\banaly[sz]e\b/i.test(statement)) {
    return "`EXPLAIN ANALYZE` runs the statement it explains, which a read never does"
  }
  const outside = PSQL_OUTSIDE_EFFECT.exec(statement)
  if (outside !== null) return `\`${outside[1]}\` acts outside the read-only transaction, so it is never called here`
  return null
}

// Whether the `-i` at `rest[i]` comes right after `-f <an allowed format>`.
function formatNamed(rest: string[], i: number): boolean {
  return i >= 2 && rest[i - 2] === "-f" && FFMPEG_FORMATS.has(rest[i - 1])
}

function formatRefusal(value: string): string {
  return `\`-f ${value}\` is not a format you may use here (these are: ${[...FFMPEG_FORMATS].sort().join(", ")})`
}

// Why `word`, as an ffmpeg or ffprobe input, is refused: it must be a plain
// file inside the project.
function mediaInputRefusal(word: string, cwd: string, project: string): string | null {
  if (/[:|]/.test(word) || !inside(word, cwd, project)) {
    return `\`${word}\` is not a plain file inside the project, the only input read here`
  }
  return null
}

// Why a `-vf` / `-af` filtergraph is refused: every filter in it must be one
// that reads and writes no file. ffmpeg reads a backslash differently inside
// quotes and out, and at each of its two levels of escaping, so a filtergraph
// holding one is refused rather than read: quote a value with `'...'` instead.
function filtergraphRefusal(graph: string): string | null {
  if (graph.includes("\\")) return "a backslash is never in a filter here: put a value holding `,` or `;` in '...' instead"
  for (const piece of splitFiltergraph(graph)) {
    const name = /^(?:\[[^\]]*\]\s*)*([A-Za-z0-9_]*)/.exec(piece.trim())![1]
    if (!FFMPEG_FILTERS.has(name)) {
      return `the filter \`${name || piece}\` is not one you may use here (these are: ${[...FFMPEG_FILTERS].sort().join(", ")})`
    }
  }
  return null
}

// The filters of a filtergraph (one with no backslash), split on the `,` and
// `;` that are not quoted.
function splitFiltergraph(graph: string): string[] {
  const pieces: string[] = []
  let current = ""
  let quoted = false
  for (const c of graph) {
    if (c === "'") quoted = !quoted
    if ((c === "," || c === ";") && !quoted) {
      pieces.push(current)
      current = ""
    } else {
      current += c
    }
  }
  pieces.push(current)
  return pieces
}

// Whether `word`, read as a path by the tool or the shell, could name
// something outside the project. A search pattern is a word like any other,
// so only a word that looks like a path is held to the project: one starting
// with `~`; an absolute one or one with a `..` step that the shell would
// expand (`/etc/*`); or one that names something on disk outside the project
// (`/etc/passwd`, `../other`, a link out). A pattern such as `/api/v1` names
// nothing and passes. The value of a `--flag=<value>` is read the same way,
// and so is every possible value glued to a cluster of short options:
// `-rf/etc/x` reads as `f/etc/x` and as `/etc/x`, since any letter but the
// first may be the one that takes it.
function readsOutside(word: string, cwd: string, project: string): boolean {
  let candidates: string[]
  if (word.startsWith("--")) {
    const eq = word.indexOf("=")
    candidates = eq < 0 ? [] : [word.slice(eq + 1)]
  } else if (word.startsWith("-")) {
    candidates = []
    for (let k = 2; k < word.length; k++) candidates.push(word.slice(k))
  } else {
    candidates = [word]
  }
  for (const candidate of candidates) {
    if (!candidate) continue
    if (candidate.startsWith("~")) return true
    if ((GLOB.test(candidate) || lexists(resolve(cwd, candidate))) && !inside(candidate, cwd, project)) return true
  }
  return false
}

// Whether any piece of `folder`, walked from `cwd` (or from `/`), is a link.
// The shell keeps the folder a `cd` goes to as typed and this check keeps it
// with its links resolved; with no link and no `..` in the way, the two are
// the same folder.
function throughLink(folder: string, cwd: string): boolean {
  let path = folder.startsWith("/") ? "/" : cwd
  for (const piece of folder.split("/")) {
    if (!piece || piece === ".") continue
    path = join(path, piece)
    try {
      if (lstatSync(path).isSymbolicLink()) return true
    } catch {
      return false
    }
  }
  return false
}

// Whether `path` is a folder, through links.
function isFolder(path: string): boolean {
  try {
    return statSync(path).isDirectory()
  } catch {
    return false
  }
}

// Whether something is at `path`, a dangling link included.
function lexists(path: string): boolean {
  try {
    lstatSync(path)
    return true
  } catch {
    return false
  }
}

const CHECKS: Record<string, Check> = {
  grep: grepRefusal,
  rg: rgRefusal,
  ffprobe: ffprobeRefusal,
  ffmpeg: ffmpegRefusal,
  psql: psqlRefusal,
  "opencode-trial": (_item, rest) => trialRefusal(rest),
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

// Whether a piece of `path` starts with `.` and holds a glob character: bash
// 3.2 to 5.1 match `..` with `.?` or `.*`, so such a piece may step out.
function dotGlob(path: string): boolean {
  return path.split("/").some((piece) => piece.startsWith(".") && GLOB.test(piece))
}

// The paths a glob names on disk, the way the shell expands it: each piece
// holding `*`, `?` or `[` is matched against the entries of the folders found
// so far, following links as the shell does (Node's own `globSync` does not
// walk into a linked folder), and a hidden entry is matched only by a piece
// starting with `.`.
function expandGlob(pattern: string, cwd: string): string[] {
  let found = [pattern.startsWith("/") ? "/" : cwd]
  for (const piece of pattern.split("/")) {
    if (!piece) continue
    if (!GLOB.test(piece)) {
      found = found.map((folder) => join(folder, piece))
      continue
    }
    found = found.flatMap((folder) => {
      let names: string[]
      try {
        names = readdirSync(folder)
      } catch {
        return []
      }
      return names.filter((name) => (piece.startsWith(".") || !name.startsWith(".")) && matchesGlob(name, piece)).map((name) => join(folder, name))
    })
  }
  return found.filter(lexists)
}

// Whether `path`, read from `cwd`, lands inside the project folder. A path
// holding `*`, `?` or `[` is read the way the shell expands it: every file it
// matches, each with its links resolved, must be inside (so `lin*/passwd`
// through a link to /etc is not), and one that matches nothing is the text
// the shell passes on as it is. `**` is never inside: zsh follows links
// through it in a way this check does not mirror.
export function inside(path: string, cwd: string, project: string): boolean {
  if (path.startsWith("~") || path.includes("**") || dotGlob(path)) return false
  const root = realpath(project)
  const matches = GLOB.test(path) ? expandGlob(path, cwd) : []
  return (matches.length ? matches : [path]).every((match) => {
    const target = realpath(resolve(cwd, match))
    return target === root || target.startsWith(root + sep)
  })
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
