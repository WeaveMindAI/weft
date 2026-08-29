# How connections work

Adding a service to weft is a JSON file.

## The one idea

A **connection** is an account somebody hooked up to an outside service. It
lives in the access store and it holds everything secret, while a project's
source holds a bare id and nothing else, which is why a credential can never
end up in git history.

Every kind of credential is the same concept: an OAuth sign-in, a pasted API
key, a GitHub App's private key, a mail server's host and user and password.
One store, one connect screen, one way for a node to use it.

## The four objects

Three of them sit somewhere and are easy to mix up, so here they are side by
side. The fourth is the one that moves, and it comes after.

```
 RECIPE                     REGISTERED APP              CONNECTION
 in the access node's       in the operator's           a row in the access store
 metadata.json              apps file

 written by the node        written by whoever runs     created when a user
 author. Any user.          this weft. Trusted.         connects, or when a node
 Untrusted input.           Holds real secrets.         publishes one for a
                                                        service it runs itself.
───────────────────────    ────────────────────────    ─────────────────────────
 describes the SERVICE,     one OAuth app this weft     one account, hooked up:
 true for everybody:        signs users in with:        · who it is
 · how a credential is      · a label                   · through which app
   acquired                 · client id + secret        · which permissions, and
 · how a request is signed  · the fixed permission        whether they are verified
 · the permission           set it asks for             · the stored values
   catalogue                · pinned sign-in            · snapshots of the recipe
 · how it is verified         addresses                   and app it was made with
 · how events arrive        · event-receiving secrets
 · which doors exist
```

And the fourth: the **`Access` value**, which is what actually flows through a
graph.

```json
{ "__weft_access__": { "accessId": "…uuid…", "service": "slack",
                       "identity": "Acme Corp" } }
```

An access node emits it, action nodes consume it. It is not a secret:
resolving it requires being authenticated as the tenant that owns the row, and
a missing row and another tenant's row give the same "not found", so existence
never leaks.

## How they link

**By the service name string, and nothing else.**

A recipe saying `"service": "slack"` reaches the apps filed under `"slack"`.
The recipe travels with every connect request and the server looks the apps up
itself. A connection snapshots both at creation, so using it later needs no
catalog and no file lookup.

Because the recipe is user-authored and the apps file is operator-owned, the
recipe may use a registered app and can never extract from it. Five rules
follow:

- **Pinned addresses.** Each registered app writes its own sign-in and token
  addresses beside its secret. A shared connect whose recipe names any other
  address is refused, so the app's credentials only ever go where the operator
  wrote.
- **Fixed permissions.** A shared connect's permission set is replaced by the
  chosen app's declared set, from the trusted file. Nothing a client sends
  widens it.
- **Secrets stay compartmentalized.** An app's client secret is snapshotted
  into its own field on the connection row. What a worker is handed comes from
  a different field, and nothing ever copies between the two, so the secret has
  no path to a node.
- **Event material joins own-door connections only.** An app's event-serving
  values (a socket token, a signing secret) merge into event resolution only
  when the app is the user's own. A registered app's material serves
  connecting, never event serving.
- **The server resolves the app.** A connect request names a door and, for the
  shared door, an app label. It never carries an app. The client is not the
  security boundary.

## Life of a connection

```
        the user clicks the access node's field
                        │
                        ▼
          doors probe: what can this weft offer?
          one option per registered app, plus
          "your own" when offered
                        │
        ┌───────────────┴───────────────┐
        ▼                               ▼
   SHARED door                     OWN door: one page with up to
   one click. The app is           three parts, whichever exist:
   resolved by label from          · mint   ("create it for me")
   the trusted file, and           · guide  (generated from the ticks)
   permissions are its             · fields (paste; always there)
   declared set.
        │                               │
        └───────────────┬───────────────┘
                        ▼
          the acquisition runs: consent, paste, a JWT mint,
          a server-to-server exchange. It runs on the broker,
          whose egress denies every private range.
                        │
                        ▼
          the verification ladder, and an identity capture
                        │
                        ▼
          one row: values, permissions and whether they are
          verified, owner, door, label, identity, snapshots
```

After that the connection appears in the picker for every project of that
tenant, and picking it stores only its id on the node.

## Doors

At most two kinds, and a door that is not offered is **hidden**, never greyed
out.

**`shared`**: a credential this weft holds. A registered app for a
consent-based service, or the runtime's own key for a key-based service, whose
calls spend its credit and say so.

**`own`**: the user brings or creates their own.

A service whose auth includes a cryptographic signing step can never offer
`shared`, and declaring both is a parse error. The shared lane substitutes the
secret into the request, and a signed request carries a hash computed **from**
the secret instead, so there is nothing to substitute.

## The verification ladder

What connect-time checking can learn about a fresh credential, best rung
first.

| Rung | What the provider tells us | Permissions recorded as |
|---|---|---|
| `reports_permissions` | what it granted, explicitly | **verified** |
| `self_introspect` | the credential describes itself | **verified** |
| `reports_validity` | only "alive", never "what" | claimed |
| `probe` | a real call's refusal, read back | claimed |
| `silent` | nothing knowable | claimed |

A check's cost is `free`, `ambiguous`, or `paid`, and anything but `free` is
**never auto-run**: the first real call surfaces the truth instead.

Later, a permission shortfall hard-fails only on a **verified** set. A claimed
set passes with a warning, because refusing would block every pasted key on
every service that reports nothing.

## Coexistence

`grants` on the recipe records how the provider behaves. You do not get to
choose it.

`coexisting` is the Google class: one token per consent, and grants are per
project.

`exclusive` is the Slack-bot and GitHub-App class: **one** grant per app and
account, and there is no way to have two. Re-consent rotates it in place, reuse inherits it, a
permission upgrade unions onto it, and every referencing project follows.

## Life of a call

```
 node body                    runtime                        store / broker
──────────────────────────────────────────────────────────────────────────────
 ctx.open(&access) ────────▶ resolve request ─────────────▶ the tenant wall
                             (id, service, what it needs)  refreshes an expired
                                                            token once, however
                                                            many callers ask
                             ◀───────────────────────────── values + auth steps
                                                            + owner
                             build ONE client:
                               timeouts, redirect and
                               address limits
                             + auth           (always)
                             + metering       (only if the service has
                                               a meter)
                             + routing        (only if the credential
                                               says to)
 conn.client() ◀──────────── the signed-in client
 …the node makes calls…
 body finishes, any way ──▶ release the lease
```

What the worker receives is everything the connection stores **except** the
store's own keep-alive material, meaning the refresh token. An app secret is
not a stored value and cannot travel at all.

Refresh is lazy, at resolution, single-flight through the row lock. A revoked
credential is a loud "needs reconnecting" error naming the fix, never a silent
retry.

## Encryption at rest

Every stored secret is sealed with AES-256-GCM under
`CREDENTIAL_ENCRYPTION_KEY`: a grant's values, an app snapshot, a pending
consent's verifier, a subscription's echo token.

A database dump alone carries no usable credential. Only non-secret row data
stays plain (service names, permission sets, value **names**, the public
client id) so queries can work on it.

Unset, a built-in development key is used with a logged warning. Set a real one
(`openssl rand -base64 32`) before storing credentials you care about, and see
[the apps file](the-apps-file.md#encryption) for what changing it later costs
you.

## Measured and billed are separate switches

Measured and billed sound like one switch, and they answer different questions.

A registered **meter** for the service means its calls are measured, whoever
pays.

The **owner** on the connection row decides whose money: the user's own
credential is measured and not billed; the runtime's is measured and billed.

A node declares neither, and node code cannot tell which combination it is
running in. The same node runs correctly in every combination.

## Where things run

| Component | Its job |
|---|---|
| **Editor** | renders the picker and the doors, sends pasted values straight to the store, runs the live requirement check when a connection is picked. Never holds an app, never decides permissions. |
| **Dispatcher** | the authenticated front door. Forwards connect verbs, serves the OAuth callback and the picker page, answers pure-database reads. |
| **Broker** | every verb whose work makes an **outbound** call to a URL the tenant influences: test calls, token exchanges, lookups, app mints, event subscribes. Its egress denies every private range, so a crafted URL aimed at an internal address dies at the network layer. |
| **Access store** | the rows, the tenant wall, the lazy refresh, the permission and value backstops. |
| **Worker** | opens connections per firing, builds the one signed-in client, runs node code. |
| **Listener** | holds event sources, resolving the connection freshly through the broker on every reconnect, so credentials are never frozen into a loop. |
