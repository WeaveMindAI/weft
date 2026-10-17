// Runs the cases of claude-code's specialist_commands hook, adapted to this
// host, plus OpenCode's own, against .opencode/specialist-commands/decide.ts
// and the plugin that calls it: `node tests/specialist-commands.test.ts`
// (Node 23.6 or newer runs TypeScript as is).
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const FOLDER = join(dirname(fileURLToPath(import.meta.url)), "..", ".opencode")
const { readLists, refusal } = await import(join(FOLDER, "specialist-commands/decide.ts"))
const plugin = (await import(join(FOLDER, "plugins/specialist-commands.ts"))).default

const project = mkdtempSync(join(tmpdir(), "weft-specialist-commands-"))
for (const dir of ["src", ".scratch/node-smith/ex", ".scratch/prompt-engineer/t", "front"]) mkdirSync(join(project, dir), { recursive: true })
writeFileSync(join(project, "src/main.weft"), "")
writeFileSync(join(project, ".scratch/prompt-engineer/t/input-1.txt"), "")
const lists = readLists()

// Trial folders the prompt-engineer would have written.
const trial = (name: string, config: unknown, extra: string[] = []) => {
  const dir = `/tmp/weft-trial-casetest-${name}`
  rmSync(dir, { recursive: true, force: true })
  mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, "opencode.json"), JSON.stringify(config))
  for (const file of extra) writeFileSync(join(dir, file), "x")
  return dir
}
const good = { agent: { trial: { mode: "primary", prompt: "{file:/p/assets/prompts/x.md}", permission: { "*": "deny" } } } }
const goodDir = trial("good", { $schema: "https://opencode.ai/config.json", ...good })
const toolsDir = trial("tools", { agent: { trial: { mode: "primary", prompt: "{file:/p/x.md}" } } })
const twoDir = trial("two", { agent: { ...good.agent, build: { mode: "primary" } } })
const agentsDir = trial("agentsmd", good, ["AGENTS.md"])
const T = "OPENCODE_DISABLE_CLAUDE_CODE_PROMPT=1 opencode run --agent trial --title trial"
const IN = "< .scratch/prompt-engineer/t/input-1.txt"

type Case = [string | null, string, string]
// The 52 cases of tangle/claude-code's hook test, as they are there.
const reference: Case[] = [
  [null, "rm -rf /", "none"],
  ["general-purpose", "rm -rf /", "none"],
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
  ["prompt-engineer", "claude -p --safe-mode --tools \"\" --no-session-persistence --model sonnet --system-prompt-file assets/prompts/x.md < .scratch/prompt-engineer/t/input-1.txt", "allow"],
  ["prompt-engineer", "claude -p --model sonnet \"hi\"", "deny"],
  ["prompt-engineer", "claude -p --safe-mode --tools \"\" --no-session-persistence < /tmp/x", "deny"],
  ["frontend-builder", "pnpm dlx sv@0.17.0 create front --template minimal --types ts --no-add-ons --no-install", "allow"],
  ["frontend-builder", "pnpm dlx evil", "deny"],
  ["frontend-builder", "pnpm exec rm -rf /", "deny"],
  ["frontend-builder", "cd front && pnpm add clsx tailwind-merge && pnpm add -D tw-animate-css shadcn-svelte@1.6.1 && pnpm install", "allow"],
  ["frontend-builder", "kill 4242", "allow"],
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
  ["frontend-builder", "weft run 'x", "deny"]
]
// The three claude-code prompt-engineer cases name `claude -p`; here the
// trial is `opencode run` (same expectations, this host's command).
const adapted: Case[] = reference.map(([agent, command, want]) => {
  if (agent === "prompt-engineer" && command.startsWith("claude -p --safe-mode") && want === "allow")
    return [agent, `${T} --dir ${goodDir} --model anthropic/claude-sonnet-4-5 ${IN}`, want]
  if (agent === "prompt-engineer" && command.startsWith('claude -p --model'))
    return [agent, 'opencode run "hi"', want]
  if (agent === "prompt-engineer" && command.includes("< /tmp/x"))
    return [agent, `${T} --dir ${goodDir} --model a/b < /tmp/x`, want]
  return [agent, command, want]
})
const host: Case[] = [
  ["prompt-engineer", "opencode models", "allow"],
  ["prompt-engineer", `${T} --dir ${toolsDir} --model a/b ${IN}`, "deny"],
  ["prompt-engineer", `${T} --dir ${twoDir} --model a/b ${IN}`, "deny"],
  ["prompt-engineer", `${T} --dir ${agentsDir} --model a/b ${IN}`, "deny"],
  ["prompt-engineer", `${T} --dir ${goodDir} --model a/b --auto ${IN}`, "deny"],
  ["prompt-engineer", `${T} --dir ${goodDir} --model a/b --agent build ${IN}`, "deny"],
  ["prompt-engineer", `${T} --dir /tmp/elsewhere --model a/b ${IN}`, "deny"],
  ["prompt-engineer", `${T} --dir /tmp/weft-trial-casetest-good/../x --model a/b ${IN}`, "deny"],
  ["prompt-engineer", `${T} --dir /tmp/weft-trial-casetest-missing --model a/b ${IN}`, "deny"],
  ["prompt-engineer", `opencode run --dir ${goodDir} --agent build hi`, "deny"],
  ["node-smith", `${T} --dir ${goodDir} --model a/b ${IN}`, "deny"],
  ["node-smith", "opencode export ses_abc", "deny"],
  ["run-digger", "weft events abc | head", "deny"],
  ["frontend-builder", "kill 4242 && kill 4243", "allow"],
  ["frontend-builder", "weft run # trailing comment", "allow"],
  ["deployer", "weft daemon logs --tail 50", "allow"],
  ["deployer", "weft target export prod", "deny"],
  ["deployer", "weft ci add", "deny"],
  ["group-builder", "weft validate --file src/main.weft<src/main.weft", "allow"],
  ["group-builder", "weft run --group 'a=1'&&weft status", "allow"],
]

let bad = 0
let total = 0
const show = (ok: boolean, line: string) => {
  total++
  if (!ok) bad++
  console.log((ok ? "ok  " : "BAD ") + line)
}

// The decision itself, for every case that names a listed agent.
for (const [agent, command, want] of [...adapted, ...host]) {
  const entry = agent ? lists[agent] : undefined
  if (!entry) continue
  const reason = refusal(command, entry, project, project)
  const got = reason === null ? "allow" : "deny"
  show(got === want, `${agent}: ${command.slice(0, 90)} -> ${got} (want ${want})${reason ? "  [" + reason + "]" : ""}`)
}

// The plugin end to end with a stand-in client: who is a specialist, and
// what a refusal looks like to the model.
const sessions: Record<string, any> = {
  main: { id: "main" },
  sub_digger: { id: "sub_digger", parentID: "main", agent: "run-digger" },
  sub_general: { id: "sub_general", parentID: "main", agent: "general" },
  sub_smith: { id: "sub_smith", parentID: "main", agent: "node-smith" },
}
const client = { session: { get: async ({ path }: any) => ({ data: sessions[path.id] }) } }
const hooks = await plugin({ client, directory: project })
const call = async (session: string, command: string, workdir?: string) => {
  try {
    await hooks["tool.execute.before"]({ tool: "bash", sessionID: session, callID: "c" }, { args: { command, workdir } })
    return "pass"
  } catch (error) {
    return "refused: " + (error as Error).message
  }
}
for (const [session, command, workdir, want] of [
  ["main", "rm -rf /", undefined, "pass"],
  ["sub_general", "rm -rf /", undefined, "pass"],
  ["sub_digger", "weft events abc --node x", undefined, "pass"],
  ["sub_digger", "weft rm --yes", undefined, "refused"],
  ["sub_smith", "weft test-node Gmail", ".scratch/node-smith", "pass"],
  ["sub_smith", "weft test-node Gmail", "/tmp", "refused"],
] as [string, string, string | undefined, string][]) {
  const got = await call(session, command, workdir)
  show(got.startsWith(want), `plugin ${session}${workdir ? " (workdir " + workdir + ")" : ""}: ${command} -> ${got.slice(0, 150)}`)
}
const nonBash = await hooks["tool.execute.before"]({ tool: "read", sessionID: "sub_digger", callID: "c" }, { args: { filePath: "/etc/x" } })
show(nonBash === undefined, "plugin leaves every tool but bash alone")

for (const name of ["good", "tools", "two", "agentsmd"]) rmSync(`/tmp/weft-trial-casetest-${name}`, { recursive: true, force: true })
rmSync(project, { recursive: true, force: true })
console.log(`passed ${total - bad} of ${total}`)
process.exit(bad ? 1 : 0)
