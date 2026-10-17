import assert from "node:assert/strict"
import test from "node:test"

const { default: plugin, REFUSAL } = await import("../.kilo/plugin/refuse-specialist-prompts.ts")
const { approved } = await import("../.kilo/lib/specialist-commands.ts")

// A client double. `sessions` maps a session id to what `session.get` returns;
// `get` and `post` can be swapped to fail; `posts` records every reply sent and
// `logs` every entry written to Kilo's log.
const fakeClient = (sessions: Record<string, any>, overrides: { get?: any; post?: any; noInner?: boolean } = {}) => {
  const posts: any[] = []
  const logs: any[] = []
  const client: any = {
    posts,
    logs,
    app: { log: async (options: any) => { logs.push(options.body) } },
    session: {
      get: overrides.get ?? (async ({ path }: any) => ({ data: sessions[path.id] })),
    },
  }
  if (!overrides.noInner) {
    client._client = { post: overrides.post ?? (async (options: any) => { posts.push(options); return { data: true } }) }
  }
  return client
}

const asked = (sessionID: string, id = "per_1") => ({
  event: { type: "permission.asked", properties: { id, sessionID, permission: "bash", patterns: ["rm -rf x"] } },
})

// The plugin also writes each failure to stderr; keep the test output clean.
const quietly = async (body: () => Promise<void>) => {
  const original = console.error
  const lines: string[] = []
  console.error = (line: string) => { lines.push(line) }
  try {
    await body()
  } finally {
    console.error = original
  }
  return lines
}

test("a prompt raised inside a subagent is rejected at once, with the message", async () => {
  const client = fakeClient({ ses_child: { id: "ses_child", parentID: "ses_main" } })
  const hooks = await plugin({ client })
  await hooks.event(asked("ses_child", "per_7"))

  assert.equal(client.posts.length, 1)
  assert.equal(client.posts[0].url, "/permission/{requestID}/reply")
  assert.deepEqual(client.posts[0].path, { requestID: "per_7" })
  assert.deepEqual(client.posts[0].body, { reply: "reject", message: REFUSAL })
  assert.equal(client.logs.length, 0)
})

test("a prompt raised in the main session is left for the user", async () => {
  const client = fakeClient({ ses_main: { id: "ses_main" } })
  const hooks = await plugin({ client })
  await hooks.event(asked("ses_main"))

  assert.equal(client.posts.length, 0)
  assert.equal(client.logs.length, 0)
})

test("other events and malformed ones are ignored", async () => {
  const client = fakeClient({ ses_child: { id: "ses_child", parentID: "ses_main" } })
  const hooks = await plugin({ client })
  await hooks.event({ event: { type: "permission.replied", properties: { sessionID: "ses_child", requestID: "per_1" } } })
  await hooks.event({ event: { type: "permission.asked", properties: { sessionID: "ses_child" } } })

  assert.equal(client.posts.length, 0)
})

test("a session.get that throws leaves the prompt standing and says so", async () => {
  const client = fakeClient({}, { get: async () => { throw new Error("server gone") } })
  const hooks = await plugin({ client })
  const stderr = await quietly(() => hooks.event(asked("ses_child", "per_2")))

  assert.equal(client.posts.length, 0)
  assert.equal(client.logs.length, 1)
  assert.equal(client.logs[0].level, "error")
  assert.match(client.logs[0].message, /could not read session ses_child, prompt per_2 left for the user: server gone/)
  assert.equal(stderr.length, 1)
})

test("a session.get that answers with an error leaves the prompt standing and says so", async () => {
  const client = fakeClient({}, { get: async () => ({ error: { name: "NotFoundError" } }) })
  const hooks = await plugin({ client })
  await quietly(() => hooks.event(asked("ses_child")))

  assert.equal(client.posts.length, 0)
  assert.match(client.logs[0].message, /could not read session ses_child/)
})

test("a reply that fails is reported, not swallowed", async () => {
  const client = fakeClient(
    { ses_child: { id: "ses_child", parentID: "ses_main" } },
    { post: async () => ({ error: { name: "Permission.NotFoundError" } }) },
  )
  const hooks = await plugin({ client })
  const stderr = await quietly(() => hooks.event(asked("ses_child", "per_3")))

  assert.match(client.logs[0].message, /could not answer prompt per_3 of subagent session ses_child/)
  assert.equal(stderr.length, 1)
})

test("a client with no inner transport is reported, not swallowed", async () => {
  const client = fakeClient({ ses_child: { id: "ses_child", parentID: "ses_main" } }, { noInner: true })
  const hooks = await plugin({ client })
  await quietly(() => hooks.event(asked("ses_child", "per_4")))

  assert.match(client.logs[0].message, /could not answer prompt per_4/)
})

test("a prompt raised by a command the specialist's own list let through is answered once", async () => {
  const client = fakeClient({ ses_child: { id: "ses_child", parentID: "ses_main" } })
  const hooks = await plugin({ client })
  approved.add("call_listed")
  try {
    await hooks.event({
      event: {
        type: "permission.asked",
        properties: { id: "per_9", sessionID: "ses_child", permission: "bash", patterns: ["pnpm run build"], tool: { messageID: "m", callID: "call_listed" } },
      },
    })
  } finally {
    approved.delete("call_listed")
  }
  assert.deepEqual(client.posts[0].body, { reply: "once" })
})

test("a prompt from a call the list did not let through is still refused", async () => {
  const client = fakeClient({ ses_child: { id: "ses_child", parentID: "ses_main" } })
  const hooks = await plugin({ client })
  await hooks.event({
    event: {
      type: "permission.asked",
      properties: { id: "per_10", sessionID: "ses_child", permission: "external_directory", patterns: ["/etc/*"], tool: { messageID: "m", callID: "call_other" } },
    },
  })
  assert.deepEqual(client.posts[0].body, { reply: "reject", message: REFUSAL })
})
