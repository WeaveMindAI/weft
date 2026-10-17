// A specialist never waits on a permission prompt.
//
// A specialist runs as a subagent while Tangle and the user are busy
// elsewhere. In the editor or the terminal UI, a prompt it raises waits until
// somebody happens to look, and nothing times it out. So every permission
// request raised inside a subagent session (a session with a parent) is
// answered "reject" the moment it is asked, with a message telling the
// specialist to do the thing another way or report BLOCKED with the command.
// Kilo hands that message to the specialist as the user's feedback on the
// refused call. The one exception is a prompt raised by a shell command that
// `.kilo/plugin/specialist-commands.ts` already checked against that
// specialist's own list in `.kilo/specialist_commands.json` and let through
// (its call id is in `approved`): that prompt is answered "once", because the
// list is what decides a specialist's commands. The main session's own requests have no parent and are left
// alone, so the user still answers those as before.
//
// The plugin `event` hook sees `permission.asked` (the request's `id` and its
// `sessionID`). The reply goes to `POST /permission/{requestID}/reply`, the
// route that takes a `message`; the SDK's public reply method
// (`postSessionIdPermissionsPermissionId`) takes none, and a refusal with no
// message reads to the specialist as a plain "no". If the session cannot be
// read or the reply cannot be sent, the prompt stays standing (a broken plugin
// never refuses something it could not check), and the failure is written to
// Kilo's log and to stderr so it is seen rather than lost.

import { approved } from "../lib/specialist-commands.ts"

export const REFUSAL =
  "never wait on a permission prompt: do it another way, or report BLOCKED with the command"

const SERVICE = "refuse-specialist-prompts"

const describe = (error: unknown) =>
  error instanceof Error ? error.message : typeof error === "string" ? error : JSON.stringify(error)

const plugin = async ({ client }: { client: any }) => {
  const report = async (message: string) => {
    console.error(`${SERVICE}: ${message}`)
    try {
      await client.app.log({ body: { service: SERVICE, level: "error", message } })
    } catch {}
  }

  return {
    event: async ({ event }: { event: { type: string; properties?: any } }) => {
      if (event.type !== "permission.asked") return
      const requestID = event.properties?.id
      const sessionID = event.properties?.sessionID
      if (typeof requestID !== "string" || typeof sessionID !== "string") return

      let session: any
      try {
        const result = await client.session.get({ path: { id: sessionID } })
        if (result?.error) throw result.error
        session = result?.data
      } catch (error) {
        await report(`could not read session ${sessionID}, prompt ${requestID} left for the user: ${describe(error)}`)
        return
      }
      if (!session?.parentID) return

      try {
        const listed = typeof event.properties?.tool?.callID === "string" && approved.has(event.properties.tool.callID)
        const result = await client._client.post({
          url: "/permission/{requestID}/reply",
          path: { requestID },
          body: listed ? { reply: "once" } : { reply: "reject", message: REFUSAL },
          headers: { "Content-Type": "application/json" },
        })
        if (result?.error) throw result.error
      } catch (error) {
        await report(`could not answer prompt ${requestID} of subagent session ${sessionID}: ${describe(error)}`)
      }
    },
  }
}

export default plugin
