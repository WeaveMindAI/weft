// Each specialist runs its own shell commands and no others.
//
// OpenCode lets an agent run any command no permission rule names, so a
// specialist's own list is made real here: a `tool.execute.before` hook on
// the `bash` tool, which runs before the tool and before its permission
// check, and refuses the call by throwing, the way OpenCode's plugin docs
// block a tool. The lists and the decision live in
// `.opencode/specialist-commands/` (`specialist_commands.json` and
// `decide.ts`).
//
// A call is a specialist's when its session has a parent (a session the
// `task` tool started) and the session's agent is a key of the lists. The
// main session, and an agent the file does not name, are left to OpenCode's
// usual permissions. A specialist's call is refused when its list cannot be
// read: its `bash` rule is `allow`, so this hook is the only thing between
// it and the shell.

import { isAbsolute, resolve } from "node:path"
import { inside, prefixOf, readLists, refusal } from "../specialist-commands/decide.ts"

const plugin = async ({ client, directory }: { client: any; directory: string }) => ({
  "tool.execute.before": async (input: { tool: string; sessionID: string }, output: { args: any }) => {
    if (input.tool !== "bash") return
    const found = await client.session.get({ path: { id: input.sessionID } })
    const session = found?.data
    if (!session) {
      throw new Error(`specialist-commands: could not read session ${input.sessionID}: ${JSON.stringify(found?.error ?? null)}`)
    }
    if (!session.parentID) return
    const agent: string | undefined = session.agent
    if (!agent) return
    let lists
    try {
      lists = readLists()
    } catch (error) {
      throw new Error(
        `specialist-commands: your command list could not be read (${(error as Error).message}). Report BLOCKED with the exact command.`,
      )
    }
    const entry = lists[agent]
    if (!entry) return
    const command = output.args?.command
    if (typeof command !== "string") throw new Error("specialist-commands: the bash call carries no command")
    const workdir = output.args?.workdir
    const cwd = typeof workdir === "string" ? (isAbsolute(workdir) ? workdir : resolve(directory, workdir)) : directory
    const reason = !inside(cwd, directory, directory)
      ? "the command runs only from a folder inside the project"
      : refusal(command, entry, cwd, directory)
    if (reason === null) return
    const allowed = entry.commands.map(prefixOf).join(", ")
    throw new Error(
      `${reason}. As ${agent} you may run only: ${allowed}. Do it another way, or report BLOCKED with the exact command.`,
    )
  },
})

export const SpecialistCommandsPlugin = plugin

export default plugin
