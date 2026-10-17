// Each specialist runs its own shell commands and no others.
//
// Kilo's permission rules in `kilo.json` cover every agent alike, so a
// read-only specialist could otherwise run anything Tangle may run. This
// plugin gives each specialist its own list instead, kept in
// `.kilo/specialist_commands.json`: a key per agent name, and under it the
// commands that agent may run. The decision itself is in
// `.kilo/lib/specialist-commands.ts`.
//
// The `tool.execute.before` hook sees every shell command before it runs: the
// `bash` tool's `command`, and the `command` of a `background_process` start
// or monitor (that tool runs a command too). For a call from inside a
// subagent session (one with a parent) whose agent the file names:
//   - every command in the line (split on &&, ||, ;, | and &) starts with one
//     of that agent's commands: the call goes on, and its call id is noted in
//     `approved`, so a permission prompt Kilo raises for it is answered "once"
//     by `.kilo/plugin/refuse-specialist-prompts.ts`;
//   - anything else: the hook throws, which stops the call, and the message
//     (naming what that agent may run) is what the specialist reads back.
// The main session's calls, and an agent the file does not name, are left to
// Kilo's usual permissions.
//
// If the session cannot be read, or the lists cannot be, the call is refused:
// a specialist's commands are allowed only by this decision, so a decision
// that could not be made is a no.

import { isAbsolute, resolve } from "node:path"
import { approved, loadLists, refusal, refusalMessage } from "../lib/specialist-commands.ts"

const SERVICE = "specialist-commands"

// The shell command a tool call is about to run, with the folder it runs in.
const shellCommand = (tool: string, args: any): { command: string; workdir?: string } | undefined => {
  if (tool === "bash" && typeof args?.command === "string") return { command: args.command, workdir: args.workdir }
  if (
    tool === "background_process" &&
    (args?.action === "start" || args?.action === "monitor") &&
    typeof args?.command === "string"
  ) {
    return { command: args.command, workdir: args.workdir }
  }
}

const plugin = async ({ client, directory }: { client: any; directory: string }) => ({
  "tool.execute.before": async (
    input: { tool: string; sessionID: string; callID: string },
    output: { args: any },
  ) => {
    const call = shellCommand(input.tool, output.args)
    if (!call) return

    let session: any
    try {
      const result = await client.session.get({ path: { id: input.sessionID } })
      if (result?.error) throw result.error
      session = result?.data
    } catch (error) {
      throw new Error(`${SERVICE}: could not tell which agent is asking, so the command is refused: ${String((error as any)?.message ?? error)}`)
    }
    if (!session?.parentID) return

    const agent = session.agent
    let lists
    try {
      lists = loadLists()
    } catch (error) {
      throw new Error(`${SERVICE}: cannot read .kilo/specialist_commands.json, so the command is refused: ${(error as Error).message}`)
    }
    if (typeof agent !== "string") {
      throw new Error(`${SERVICE}: this subagent session names no agent, so the command is refused`)
    }
    const entry = lists[agent]
    if (!entry) return

    const cwd = call.workdir ? (isAbsolute(call.workdir) ? call.workdir : resolve(directory, call.workdir)) : directory
    const reason = refusal(call.command, entry, cwd, directory)
    if (reason !== null) throw new Error(refusalMessage(agent, reason, entry))
    approved.add(input.callID)
  },
  "tool.execute.after": async (input: { callID: string }) => {
    approved.delete(input.callID)
  },
})

export default plugin
