# Changelog

What changed between versions of weft that you need to know about when you
update. To update, pull and run `./setup.sh` again.

## Unreleased: weft no longer needs Kubernetes

**Breaking: your run history does not come along.**

Weft used to run on a local Kubernetes cluster. It now runs as one process
next to Docker on your machine, and on Google Cloud as Cloud Run services plus
one small VM. The database behind it starts over with this version, so the old
one cannot be carried forward.

If you installed weft before this version, `./setup.sh` finds the old install
and stops to ask before it goes on:

```
an older weft (the one that ran on a local Kubernetes cluster) is on this machine,
and this version cannot carry it forward. It is going to be wiped: the projects on
your disk are not touched, but their run history, versions and stored connections are.
  Do you want to proceed? [Y/n]
```

If you say yes, it wipes the old install (`scripts/scrub-old-install.sh`) and
carries on with a normal install. If you say no, nothing is touched and nothing
is installed.

What goes:
- The old cluster.
- Every project's run history and versions.
- Your stored connections: you connect your accounts again the first time a
  program asks.
- Every container, volume and image the old weft made, your programs' own
  infrastructure (and its data) included.
- Every named install on the machine (a test cell, say), with its database.
  The question lists the ones it found.

What stays:
- Your project folders. `weft run` in each one registers it again.
- Whether you turned on a public address with `--public-url`.
- The assistant you picked for `weft new`.

If you would rather wipe first by hand, or `./setup.sh` runs without a
terminal to ask on, run `scripts/scrub-old-install.sh` yourself, then
`./setup.sh`. On a machine that never had the old weft, or that was already
wiped, `./setup.sh` is a normal install or update and asks nothing.

**weft moves to new ports.**

Weft now answers at `http://127.0.0.1:14111` (it was 9999). Open the dashboard
there, and point anything you configured by hand at it. The VS Code extension
follows on its own unless you set `weft.dispatcherUrl` yourself (if yours
still says the old `http://localhost:9999`, it is ignored and the extension
offers to remove it). The browser
extension remembers the address with each token you saved, so remove those
tokens and add them again.

If you use a Cloudflare tunnel, point it at `127.0.0.1:14112` (it was 9998).

If you want weft on another port, set its variable once when you run
`./setup.sh` (`WEFT_PUBLIC_PORT`, `WEFT_OUTSIDE_PORT`, `WEFT_INTERNAL_PORT`
or `WEFT_POSTGRES_PORT`). Weft saves the ports it runs on in
`~/.local/share/weft/ports.json`, so the port stays moved on later runs and
the CLI and the VS Code extension find it there.
`WEFT_SEAWEED_PORT` moves the object store, and is read on every run.
