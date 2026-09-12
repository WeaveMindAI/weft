# Events from a service

A Slack message or an incoming email can start a program through a trigger.
How that event reaches weft depends on the service and on the connection you
picked.

## Does it need a public address?

These are the triggers people hit this with. Any other one says which
transports it can use in its own description in the picker.

Some transports dial out from weft and hold the line open. Others need the
provider to send a request in, which means weft has to be reachable.

| Trigger | How the event arrives |
|---|---|
| Slack Receive Message | Socket Mode if the connection has an app-level token, otherwise Slack posts to weft over HTTP |
| Slack App Messages | Socket Mode, across every workspace your app is installed in |
| Google Drive changes | Incoming webhooks |
| Email arrivals | An outgoing IMAP connection |

An outgoing connection works from behind a home router. A webhook does not.

When you activate the project, the trigger picks whichever transport its
connection can actually support. If none of them can work, activation stops
and says what is missing.

## "This trigger needs your weft to be reachable from the internet"

That is the activation error, and it means two things at once: the service can
only deliver events by sending requests in, and your weft has no public
address yet.

```bash
./setup.sh --public-url
```

Then activate again. If the service also offers an outgoing transport, giving
it what it needs is the other way out. For Slack that means pasting an
app-level token on the connection, which switches it to Socket Mode and drops
the public-address requirement entirely.

For a stable hostname and what the tunnel actually exposes, read [a public
address](public-address.md).

## Setting up the service side

**Slack Socket Mode.** Turn on Socket Mode in the app settings, create an
app-level token with `connections:write`, and give weft that token when you
connect your own app. Under Event Subscriptions, subscribe the bot to
`message.channels`, plus `reaction_added`, `reaction_removed` and
`file_shared` if you are using those triggers. Slack's own [Socket Mode
setup](https://docs.slack.dev/tools/python-slack-sdk/socket-mode/) covers
their side. Slack App Messages is the one you want if you are distributing the
app and want events from every install.

**Email.** Supply the mailbox's IMAP settings and credentials, and check the
provider allows IMAP for that account, since several turn it off by default.

**Webhooks.** Get a public address before you activate. Google Drive has a
second requirement: the address has to be on a domain you have verified in the
app's Google console, or Google refuses to create the watch.

## Registering a webhook address

Use the address `weft daemon status` prints. The path on the end of it is
fixed by the service you are connecting. For the catalog's Slack app that is
`<public-address>/events/slack/messages` for messages and
`<public-address>/events/slack/interactions` for interactions. The signing
secret goes in [the apps file](the-apps-file.md).

Some services let weft create and renew the subscription through their API, so
you do nothing. Others want you to type a request URL into their console
yourself. To find out which one a service does, go and read [declare
events](writing-a-service.md#declare-events).

If your public hostname changes, go and read [changing or closing the
tunnel](public-address.md#changing-or-closing-the-tunnel), then update any
URLs you registered by hand.
