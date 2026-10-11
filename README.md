# paseo-lite

A Rust rewrite of the [Paseo](https://github.com/getpaseo/paseo) daemon for low-spec machines
(small VMs, Raspberry Pi, free-tier cloud instances). It runs **Claude Code** and **Codex** agents
across all your projects from a single process, and reuses Paseo's **end-to-end encrypted relay**,
so you can pair it with the Paseo apps exactly like the Node daemon.

## What's different from upstream

| | Paseo (Node) | paseo-lite |
|---|---|---|
| Daemon runtime | Node.js, multi-package | one static Rust binary, single-threaded tokio runtime |
| Codex | one `codex app-server` **per agent**, plus throwaway ones for model listing | **one shared** `codex app-server` for every agent in every project; threads are routed by `threadId`, each with its own `cwd`; the model list is cached on disk |
| Claude Code | one long-lived `claude` child per agent | **one shared** `claude` process (`PASEO_CLAUDE_MAX_LIVE`); a process is bound to one session and `cwd`, so switching agents replaces it (`--resume`) |
| One CLI at a time | both run freely | only one CLI has a process at any time. All turns from every project and both providers share one FIFO queue. The active CLI stays warm (pre-started at launch for the last used provider). When the next queued turn needs the other CLI, it waits for running turns to finish, the active CLI is shut down, and the other one cold-starts and stays warm until the next switch. Codex model listing never starts Codex while Claude Code is active |
| Timelines | in memory | append-only JSONL on disk; streamed chunks merged into one row; unloaded from memory after `PASEO_TIMELINE_UNLOAD_SECS` idle; tool output clipped to 16 KiB |
| Projects | — | a project is just a directory; adding one costs no process and no memory until an agent runs a turn |

Idle cost is the daemon plus one warm process of the active CLI (`claude` or `codex app-server`). Set `PASEO_CODEX_KEEP_WARM=0` / `PASEO_CLAUDE_KEEP_WARM=0` to start them on demand and release them after `PASEO_CODEX_IDLE_SECS` / `PASEO_CLAUDE_IDLE_SECS` instead.

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
  `fetch_workspaces` and `directory_suggestions`;
- agent controls: `update_agent` (rename, labels), `set_agent_thinking` (`claude --effort` / Codex `effort`),
  `refresh_agent`, `resume_agent`, `list_commands` (built-ins plus `.claude/commands` and `.claude/skills`),
  `wait_for_finish`, `agent.timeline.list_prompts`, `list_provider_features`, `set_agent_feature` (declined);
- daemon and projects: `daemon.get_status`, `get_daemon_config` / `set_daemon_config` (custom models via `providers.<id>.additionalModels`, `appendSystemPrompt`; saved in `daemon-config.json`), `diagnostics`,
  `project.rename`, `project.remove` / `archive_workspace` (archive the agents and clean up the folder, see below), `workspace.title.set`,
  `workspace.clear_attention`.

Anything else (git actions, file editing, terminals, plugins, voice, schedules, Hub, worktrees) answers with
`rpc_error` / `unsupported`. The official app is expected to handle the core agent flow. Screens built
on unsupported features will show errors.

**Removing a project** (or archiving its last workspace) archives its agents and deletes the local folder
when that can't lose work: it must be a git repository with no uncommitted changes, no stashes, no
other worktrees, every commit on a remote, and no ignored files beyond regenerable ones
(`node_modules`, `target`, `dist`, `.venv`, …), so a `.env` keeps it. A folder paseo-lite created
that is still empty is deleted without checks. Otherwise the folder is kept and the app is told why.
Whatever the policy, only folders strictly inside your home directory are deleted, never one that
holds paseo-lite's data or another project. See `PASEO_DELETE_PROJECT_FILES`.

**Notifications** work like upstream: the app registers its Expo push token, and when an agent
finishes or needs permission, a client that's looking at that agent gets nothing, the most recently
active client gets an in-app notice, and if nobody is active (phone locked, app closed) the phone
gets a push notification via Expo.

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
| `PASEO_CLAUDE_MAX_LIVE` | `1` | max concurrent `claude` processes; extra prompts queue |
| `PASEO_CLAUDE_KEEP_WARM` | `true` | keep the `claude` process warm and pre-start it |
| `PASEO_CLAUDE_IDLE_SECS` | `90` | keep-warm time after a Claude turn (when keep-warm is off) |
| `PASEO_CODEX_IDLE_SECS` | `300` | idle time before the shared app-server exits (when keep-warm is off) |
| `PASEO_CODEX_MAX_ACTIVE` | `1` | max concurrent Codex turns; extra prompts queue |
| `PASEO_CODEX_KEEP_WARM` | `true` | start the app-server with the daemon and keep it up |
| `PASEO_AGENT_NICE` | `10` | CPU niceness of `claude` / `codex` and everything they run (0 = off) |
| `PASEO_AGENT_OOM_SCORE_ADJ` | `500` | OOM score for agent processes, so the kernel kills them before the daemon (0 = off) |
| `PASEO_AGENT_LOW_IO` | `true` | lowest best-effort I/O priority for agent processes |
| `PASEO_AUTO_COMPACT_PCT` | `70` | after a turn, compact the context (`/compact`, Codex `thread/compact/start`) once it fills this % of the window (0 = off) |
| `PASEO_RETRY_MAX` | `3` | automatic retries of a turn that fails with a transient error, returns nothing, or stops in the middle of a tool call; a retry after partial output asks the model to continue, otherwise the prompt is resent (0 = off) |
| `PASEO_RETRY_BASE_SECS` | `5` | delay before the first retry; doubles each attempt, max 60s |
| `PASEO_STALL_SECS` | `600` | a running turn with no output for this long (and no tool call in progress, no pending permission) is treated as stuck and retried (0 = off) |
| `PASEO_CLAUDE_CONTEXT_WINDOW` | `200000` | Claude context window used for that check (`[1m]` models use 1M) |
| `PASEO_DELETE_PROJECT_FILES` | `safe` | what removing a project does to its folder: `safe` deletes it only when nothing would be lost (see above); `all` deletes any project folder inside your home; `never` keeps everything |
| `PASEO_TIMELINE_UNLOAD_SECS` | `600` | drop idle timelines from memory |
| `PASEO_DEFAULT_SYSTEM_PROMPT` | built-in low-resource note | instructions added to every agent (Claude `--append-system-prompt`, Codex `developerInstructions`); the default tells agents not to run, serve or test the app locally on a small VM. Set to an empty string to disable |

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
