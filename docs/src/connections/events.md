# Events from a service

A trigger that waits for a Slack message, a new row in a sheet, a page changing
in Notion. The service has something to tell you, and there are two roads it
can travel.

## The two roads

**Dial out.** weft makes an authenticated call that hands back a socket
address, connects out, and holds the line. Nothing needs to reach your machine,
so this works from a laptop behind a router.

**Dial in.** The service posts each event to an address you published. That
needs [a public address](../build/public-address.md), and it is the only road a
shared application can use, and the only road most services offer at all.

You never choose. You name a connection and a filter, and the transport follows
from what the service declares and what your environment can serve. Dial out
wins when it is available, because it needs nothing from you.

## What a topic declares

A service declares a map of topics, because one provider can genuinely report
along several independent channels at once.

| Key | What it says |
|---|---|
| `fields` | The named facts of one event, and where each lives in the payload. A dotted path, or `header:X-Some-Header` |
| `account` | Which stored value names the account on a connection, and where the same id sits on an event. That pair is what routes a shared application's events to the right connection |
| `socket` | Present when the service offers dial out |
| `webhook` | Present when the service dials in |

Everything downstream works on the **names**. Subscriptions filter on them and
triggers fan them out, so neither a filter nor a trigger node ever writes a
provider's path.

## Receiving a push

A `webhook` topic declares how the provider's pushes are handled:

| Key | What it is |
|---|---|
| `verify` | How a push is proved real: an HMAC, an Ed25519 or ECDSA signature, an API key, OIDC, or a token weft minted |
| `handshake` | The one-off proof of ownership: echo a field from the body, echo a query parameter, or call a URL back |
| `event_path` | Where the event sits inside the envelope |
| `route_by` | Whether the event names the provider account, or an id weft minted |
| `decode` | For an event wrapped in base64 or a form encoding |
| `subscribe` | The calls that create, renew and cancel the subscription |

**The secret is never in the recipe.** Every scheme is a protocol with holes,
and the material comes from the operator's application config, or from a token
weft minted. That is what lets a recipe live in source.

## What activation does

For a dial-in topic, **the first subscribe happens then**. So a missing public
address or a provider refusing is that activation's error, not a mystery three
days later:

```text
this trigger cannot be served on the 'slack' connection: the service declares
no transport for this topic
```

And the one you are most likely to meet:

```text
this trigger needs 'slack' to deliver events TO your weft, which requires your
weft to be reachable from the internet, and it is not. If you accept making its
public trigger surface reachable, rerun ./setup.sh --public-url and re-activate;
or use a connection that supports dialing out (your own provider app), where
the service offers one.
```

A decision that a trigger cannot be served is final for that registration.
Retrying would work out the same facts again. Reconnecting the account
re-registers it and works them out afresh.

If a topic is not in the connection's snapshot, the account predates it:

```text
'slack' declares no event topic named 'reactions'; reconnect the account (an
older connection may predate the topic)
```

## Renewal

Most providers expire a subscription. The recipe says the margin, and weft
re-subscribes before the deadline.

**A renewal is a fresh subscribe**, with a new id and a new token, because the
providers surveyed have no extend verb, and reusing the token would make the
old and new channels impossible to tell apart.

If a topic declares no unsubscribe, deactivating cannot hurry it and the
subscription lapses on its own expiry.

## An application-wide subscription

Some apps want every install's events rather than one account's. That only
works over dial out, because a push receiver routes each event to one account's
connections and could never deliver "every install of your app".

```text
an app-wide subscription only works over a dial-out transport, and this topic
declares none
```

## Which triggers need a public address

Only the ones where the provider pushes. A timer, a poller, a socket weft dials
out to and a form somebody fills in all work with nothing reachable from
outside.

For the address itself, go and read
[a public address](../build/public-address.md). For declaring a topic, go and
read [declaring a service](declaring-a-service.md).
