# paseo-lite

A Rust rewrite of the [Paseo](https://github.com/getpaseo/paseo) daemon for low-spec machines
(small VMs, Raspberry Pi, free-tier cloud instances). It runs **Claude Code** and **Codex** agents
across all your projects from a single process, and reuses Paseo's **end-to-end encrypted relay**,
so you can pair it with the Paseo apps exactly like the Node daemon.

## What's different from upstream

| | Paseo (Node) | paseo-lite |
|---|---|---|
| Daemon runtime | Node.js, multi-package | one static Rust binary, single-threaded tokio runtime |
| Codex | one `codex app-server` **per agent**, plus throwaway ones for model listing | **one shared** `codex app-server` for every agent in every project; threads are routed by `threadId`, each with its own `cwd`; the process exits after `PASEO_CODEX_IDLE_SECS` with nothing running and threads are `thread/resume`d on demand |
| Claude Code | one long-lived `claude` child per agent | a child exists only while useful: spawned on prompt (`--resume` for existing sessions), kept warm for `PASEO_CLAUDE_IDLE_SECS`, then released; at most `PASEO_CLAUDE_MAX_LIVE` across all projects (oldest idle one is evicted, otherwise the prompt waits for a slot) |
| Timelines | in memory | append-only JSONL on disk; streamed chunks merged into one row; unloaded from memory after `PASEO_TIMELINE_UNLOAD_SECS` idle; tool output clipped to 16 KiB |
| Projects | — | a project is just a directory; adding one costs no process and no memory until an agent runs a turn |

Idle cost is the daemon itself: no agent processes run until you send a prompt.

## Relay and pairing

The relay code is a byte-compatible port of `@getpaseo/relay`:

- relay protocol v2: one control socket plus one data socket per connected client,
  `wss://<relay>/ws?serverId=…&role=server&v=2[&connectionId=…]`;
- `e2ee_hello` / `e2ee_ready` handshake, Curve25519 + XSalsa20-Poly1305 (NaCl `box`),
  bundle `nonce(24) || ciphertext`, base64 text frames;
- the same `server-id` and `daemon-keypair.json` files and the same pairing link format
  (`https://app.paseo.sh/#offer=…`).

The default relay is `relay.paseo.sh:443`. You can point it at a self-hosted
[paseo-relay](https://github.com/getpaseo/paseo-relay) with `PASEO_RELAY_ENDPOINT`.

## Protocol compatibility

paseo-lite speaks Paseo's core WebSocket protocol subset with the upstream message names and shapes:

- `hello` → `status/server_info` handshake, plus `ping`/`pong`;
- agents: `fetch_agents`, `fetch_agent`, `fetch_agent_history`, `create_agent` (with `initialPrompt`),
  `send_agent_message`, `cancel_agent`, `archive_agent`, `delete_agent`, `clear_agent_attention`;
- timeline and permissions: `fetch_agent_timeline` (projected rows with seq ranges),
  `agent_permission_request` / `_response` / `_resolved`;
- live events: `agent_update` and `agent_stream`;
- providers, workspaces and directories: `set_agent_mode`, `set_agent_model`, provider models/modes/snapshot,
  `fetch_workspaces` and `directory_suggestions`.

Anything else (git or checkout, terminals, plugins, voice, schedules, Hub, worktrees) answers with
`rpc_error` / `unsupported`. The official app is expected to handle the core agent flow. Screens built
on unsupported features will show errors.

paseo-lite also serves a small built-in web UI at `http://<listen>/` (one HTML file, no build step).
It covers the full supported flow: projects, agents, live timeline, approvals, modes, stop and archive.

## Build

```sh
cargo build --release        # binary: target/release/paseo-lite
```

The release profile optimises for size (`opt-level="z"`, LTO, stripped). You can cross-compile
on a bigger machine or in CI (see `.github/workflows/build.yml`) and copy the binary over. Building
on a 1 GB VM is slow.

## Run

```sh
paseo-lite                    # 127.0.0.1:6768 + relay; prints a pairing QR/link
paseo-lite --no-relay         # direct connections only (LAN/Tailscale/SSH tunnel)
paseo-lite pair               # print the pairing link again
PASEO_LISTEN=0.0.0.0:6768 PASEO_PASSWORD=secret paseo-lite
```

`claude` and/or `codex` must be on `PATH` and already logged in. To use other binaries, set
`PASEO_CLAUDE_BIN` / `PASEO_CODEX_BIN`.

| Variable | Default | Meaning |
|---|---|---|
| `PASEO_LITE_HOME` | `~/.paseo-lite` | state: agents, timelines, server id, keypair |
| `PASEO_LISTEN` | `127.0.0.1:6768` | HTTP/WS listen address |
| `PASEO_PASSWORD` | – | required for direct (non-relay) clients |
| `PASEO_RELAY_ENABLED` (alias `PASEO_RELAY`) | `true` | enable the outbound relay |
| `PASEO_HOSTNAMES` | – | comma-separated extra Host names for direct connections (`paseo.example.com,.lan`); IPs and localhost always allowed, others get `403 Host not allowed` |
| `PASEO_LOG_CONSOLE_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `PASEO_RELAY_ENDPOINT` | `relay.paseo.sh:443` | relay `host:port` |
| `PASEO_RELAY_USE_TLS` | `true` for `:443` | `wss` vs `ws` |
| `PASEO_RELAY_PUBLIC_ENDPOINT` / `_USE_TLS` | same as above | what goes into the pairing offer |
| `PASEO_APP_BASE_URL` | `https://app.paseo.sh` | pairing link base |
| `PASEO_CLAUDE_MAX_LIVE` | `3` | max concurrent `claude` processes |
| `PASEO_CLAUDE_IDLE_SECS` | `90` | keep-warm time after a Claude turn |
| `PASEO_CODEX_IDLE_SECS` | `300` | idle time before the shared app-server exits |
| `PASEO_TIMELINE_UNLOAD_SECS` | `600` | drop idle timelines from memory |

A systemd unit is in `contrib/paseo-lite.service`.

## Layout

```
src/main.rs              entry, runtime, signals
src/manager.rs           agent state actor: turns, queueing, timeline merge, persistence
src/rpc.rs               Paseo session RPC handlers
src/snapshot.rs          wire projections (agent snapshot, workspace, timeline entry)
src/session.rs           per-connection hello/auth + fan-out (transport agnostic)
src/server.rs            HTTP + /ws + embedded UI
src/relay.rs, e2ee.rs    relay v2 client + NaCl box channel
src/providers/claude*.rs Claude Code pool + stream-json parser
src/providers/codex*.rs  shared Codex app-server client + event mapping
src/store.rs             agents.json + JSONL timelines
web/index.html           built-in web UI
```
