# Coding agents

This package drives the coding agents people already use (Claude Code,
Codex, and the others listed in each node's description) from a weft
program. One agent node is one conversation; `AskAgent` talks to it,
`AnswerAgent` puts its questions to a person, and `conversation.weft` is a
ready loop of the two you can include or copy.

For how to use them, go and read [Driving a coding
agent](https://github.com/WeaveMindAI/weft/blob/main/docs/src/build/coding-agents.md).

What is inside, for whoever works on it:

- `agent.rs`: what every agent node shares. It asks for the agent server as a
  process on the person's machine, or as a program inside the Computer the
  node is wired to, configures it with the node's settings and login, and
  opens the run's `stream` bus, staying on it until a `CloseBus` ends it and
  handing the agent a fresh login each time it asks for one there.
- `server/`: the agent server, Python's standard library only. It owns the
  agent's process, its queue of turns, its numbered events and the call it is
  blocked on, so all of it outlives a worker. `agents.json` holds each agent's
  facts (how it installs, how a login reaches it, its permission settings),
  and each agent has an adapter translating its own protocol. Its tests run
  with `python3 test_agent_server.py`, against stand-ins that replay each
  agent's recorded protocol, so nothing needs an account.
