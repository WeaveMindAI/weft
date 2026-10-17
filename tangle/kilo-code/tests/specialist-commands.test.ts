import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdirSync, mkdtempSync, realpathSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import test from "node:test"

const { default: plugin } = await import("../.kilo/plugin/specialist-commands.ts")
const { approved, tokenize } = await import("../.kilo/lib/specialist-commands.ts")

const project = realpathSync(mkdtempSync(join(tmpdir(), "specialist-commands-")))
mkdirSync(join(project, ".scratch", "node-smith", "ex"), { recursive: true })
mkdirSync(join(project, "src"), { recursive: true })

// `agent`: null for the main session, else the subagent's agent name.
// `want`: "allow" (goes on and is noted as approved), "deny" (the hook throws),
// "none" (goes on, left to Kilo's usual permissions).
type Case = [agent: string | null, command: string, want: "allow" | "deny" | "none", tool?: string, extra?: any]

const cases: Case[] = [
  // The cases tangle/claude-code's hook passes, with Kilo's agent names.
  [null, "rm -rf /", "none"],
  ["general", "rm -rf /", "none"],
  ["run-digger", "weft events abc --node x --full", "allow"],
  ["run-digger", "weft rm --yes", "deny"],
  ["run-digger", "weft events abc --on prod", "deny"],
  ["run-digger", "weft --on prod events abc", "deny"],
  ["run-digger", "weft events abc --json > out.json", "deny"],
  ["run-digger", "weft events abc 2>&1", "deny"],
  ["run-digger", "weft events abc | jq .", "deny"],
  ["run-digger", "weft events $(cat x)", "deny"],
  ["run-digger", "timeout 15 weft status --json", "allow"],
  ["run-digger", "weft daemon restart", "deny"],
  ["run-digger", "weft daemon logs --tail 200", "allow"],
  ["group-builder", "weft validate --file src/main.weft < src/main.weft", "allow"],
  ["group-builder", "weft validate --file src/main.weft < /etc/passwd", "deny"],
  ["group-builder", "weft run --group 'triage={\"text\":\"a && b; c\"}'", "allow"],
  ["group-builder", "weft run --group 'x={}' && rm -rf src", "deny"],
  ["node-smith", "weft test-node Gmail", "allow"],
  ["node-smith", "weft test-node Gmail --tier live", "deny"],
  ["node-smith", "weft test-node --tier=live", "deny"],
  ["node-smith", "weft test-node --yes", "deny"],
  ["node-smith", "weft infra terminate --yes", "allow"],
  ["node-smith", "cd .scratch/node-smith/ex && weft infra start", "allow"],
  ["node-smith", "cd /tmp && weft infra start", "deny"],
  ["node-smith", "cd ../.. && ls", "deny"],
  // The prompt-engineer's trial is Kilo's own script here, never `claude -p`.
  ["prompt-engineer", "python3 .kilo/scripts/try_prompt.py --model kilo/~anthropic/claude-sonnet-latest --prompt assets/prompts/x.md --input .scratch/prompt-engineer/t/input-1.txt", "allow"],
  ["prompt-engineer", 'claude -p --model sonnet "hi"', "deny"],
  ["prompt-engineer", "python3 .kilo/scripts/try_prompt.py --models < /tmp/x", "deny"],
  ["frontend-builder", "pnpm dlx sv@0.17.0 create front --template minimal --types ts --no-add-ons --no-install", "allow"],
  ["frontend-builder", "pnpm dlx evil", "deny"],
  ["frontend-builder", "pnpm exec rm -rf /", "deny"],
  ["frontend-builder", "cd front && pnpm add clsx tailwind-merge && pnpm add -D tw-animate-css shadcn-svelte@1.6.1 && pnpm install", "allow"],
  // No `kill` on Kilo: the dev server stops with background_process's own stop.
  ["frontend-builder", "kill 4242", "deny"],
  ["frontend-builder", "kill -9 -1", "deny"],
  ["frontend-builder", "kill 1", "deny"],
  ["frontend-builder", "openssl rand -base64 32", "allow"],
  ["frontend-builder", "weft infra env db --into front/.env --set A=B", "allow"],
  ["frontend-builder", "weft token mint", "deny"],
  ["designer", "pnpm run dev --port 5174 --strictPort", "allow"],
  ["designer", "node x.mjs", "deny"],
  ["deployer", "weft activate --on prod", "allow"],
  ["deployer", "timeout 360 weft resync --on prod --mode park", "allow"],
  ["deployer", "weft target export prod --github", "deny"],
  ["deployer", "weft login prod", "deny"],
  ["deployer", "weft daemon restart", "deny"],
  ["deployer", "weft branch abc --on prod --discard", "deny"],
  ["deployer", "weft workers set --min-instances 1 --on prod", "deny"],
  ["deployer", "weft workers --on prod", "allow"],
  ["deployer", "curl https://x", "deny"],
  ["red-teamer", "weft describe-nodes --node X --compact", "allow"],
  ["red-teamer", "weft run", "deny"],
  ["frontend-builder", "weft run 'x", "deny"],
  // Kilo's own commands and tools.
  ["designer", "pnpm run dev --port 5174 --strictPort", "allow", "background_process", { action: "start" }],
  ["designer", "rm -rf design-lab", "deny", "background_process", { action: "start" }],
  ["frontend-builder", "pnpm run build", "allow", "background_process", { action: "monitor" }],
  ["designer", "", "none", "background_process", { action: "stop", id: "bp_1" }],
  ["run-digger", "python3 .kilo/scripts/specialist_quiet.py ses_abc", "deny"],
  ["run-digger", "kilo export ses_abc", "deny"],
  ["run-digger", 'kilo db "SELECT 1"', "deny"],
  ["prompt-engineer", "python3 .kilo/scripts/try_prompt.py --models", "allow"],
  ["prompt-engineer", "python3 .kilo/scripts/specialist_quiet.py ses_abc", "deny"],
  ["node-smith", "weft test-node Gmail", "allow", "bash", { workdir: ".scratch/node-smith/ex" }],
  ["node-smith", "cd .. && weft infra start", "allow", "bash", { workdir: ".scratch/node-smith/ex" }],
  ["node-smith", "weft infra start", "none", "read"],
]

const clientFor = (agent: string | null) => ({
  session: {
    get: async () => ({ data: agent === null ? { id: "ses_main" } : { id: "ses_child", parentID: "ses_main", agent } }),
  },
})

let n = 0
for (const [agent, command, want, tool = "bash", extra = {}] of cases) {
  const index = ++n
  test(`${index}. ${agent ?? "main session"} via ${tool}: ${command.slice(0, 70)} -> ${want}`, async () => {
    const hooks = await plugin({ client: clientFor(agent), directory: project })
    const callID = `call_${index}`
    let thrown: Error | undefined
    try {
      await hooks["tool.execute.before"]({ tool, sessionID: "s", callID }, { args: { command, ...extra } })
    } catch (error) {
      thrown = error as Error
    }
    const got = thrown ? "deny" : approved.has(callID) ? "allow" : "none"
    approved.delete(callID)
    assert.equal(got, want, thrown?.message)
    if (thrown) assert.match(thrown.message, /you may run only|refused/)
  })
}

test("a refusal names the specialist's list", async () => {
  const hooks = await plugin({ client: clientFor("red-teamer"), directory: project })
  await assert.rejects(
    hooks["tool.execute.before"]({ tool: "bash", sessionID: "s", callID: "c" }, { args: { command: "weft run" } }),
    /`weft run` is not on your list\. As red-teamer you may run only: weft describe-nodes, weft validate, weft parse, weft status, weft daemon logs\. Do it another way, or report BLOCKED with the exact command\./,
  )
})

test("a session that cannot be read refuses the command", async () => {
  const client = { session: { get: async () => ({ error: { name: "NotFoundError" } }) } }
  const hooks = await plugin({ client, directory: project })
  await assert.rejects(
    hooks["tool.execute.before"]({ tool: "bash", sessionID: "s", callID: "c" }, { args: { command: "weft status" } }),
    /could not tell which agent is asking/,
  )
})

test("a subagent session that names no agent refuses the command", async () => {
  const client = { session: { get: async () => ({ data: { id: "ses_child", parentID: "ses_main" } }) } }
  const hooks = await plugin({ client, directory: project })
  await assert.rejects(
    hooks["tool.execute.before"]({ tool: "bash", sessionID: "s", callID: "c" }, { args: { command: "weft status" } }),
    /names no agent/,
  )
})

test("tool.execute.after forgets the call", async () => {
  const hooks = await plugin({ client: clientFor("run-digger"), directory: project })
  await hooks["tool.execute.before"]({ tool: "bash", sessionID: "s", callID: "c_after" }, { args: { command: "weft status" } })
  assert.ok(approved.has("c_after"))
  await hooks["tool.execute.after"]({ callID: "c_after" })
  assert.ok(!approved.has("c_after"))
})

// The tokenizer must read a line exactly as Python's shlex does in the
// claude-code hook, so both folders split the same line the same way.
test("the tokenizer agrees with Python's shlex", () => {
  const lines = [
    "weft run --group 'triage={\"text\":\"a && b; c\"}'",
    'echo "a \\"b\\" c" d\\ e',
    "a&&b||c;d|e&f |& g",
    "weft events abc 2>&1",
    "x >> y < z",
    "a#b c",
    "# all comment",
    "a 'b''c' \"\" ''",
    "pnpm dlx sv@0.17.0 create front",
    'say "it\'s" \'"q"\'',
    "back\\\\slash \"in \\\\ quote\" 'single \\\\ kept'",
    "(sub) {x} a=b ~/p ./q *.ts ?x",
    "tab\tsep  two   spaces",
    'mixed"quo"ted\'parts\'',
    "cd .scratch/node-smith/ex && weft infra start",
    "trailing &",
    "\"unicode é ñ\" ß",
  ]
  const script =
    "import json,shlex,sys\n" +
    "out=[]\n" +
    "for line in json.load(sys.stdin):\n" +
    "    lx=shlex.shlex(line,posix=True,punctuation_chars=True); lx.whitespace_split=True\n" +
    "    out.append(list(lx))\n" +
    "print(json.dumps(out))\n"
  const done = spawnSync("python3", ["-c", script], { input: JSON.stringify(lines), encoding: "utf8" })
  assert.equal(done.status, 0, done.stderr)
  const python = JSON.parse(done.stdout)
  lines.forEach((line, i) => assert.deepEqual(tokenize(line), python[i], line))
  for (const broken of ["weft run 'x", 'a "b', "a\\"]) {
    assert.throws(() => tokenize(broken))
    const check = spawnSync(
      "python3",
      ["-c", "import shlex,sys\nlx=shlex.shlex(sys.argv[1],posix=True,punctuation_chars=True); lx.whitespace_split=True\nlist(lx)", broken],
      { encoding: "utf8" },
    )
    assert.notEqual(check.status, 0, broken)
  }
})
