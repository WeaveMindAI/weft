---

These maps come from three read-throughs of the current code. Every name, table and route in them is what the code says today. Nothing has run on GCP for real yet, so read the GCP maps as the code's intent.

## 0. The big picture

```
LOCAL (your machine)
═══════════════════════════════════════════════════════════════════════════════════
 you: weft CLI · VS Code ext · browser ext · webhooks · live callers (HTTP/WS)
        │                                │
        │ 127.0.0.1:9999 (public)        │ internet ──► cloudflared container
        │                                │              (weft-tunnel, host network)
        │                                ▼
        │                        127.0.0.1:9998 (outside: webhooks, /connect,
        │                                        /live, /signal/.., forms)
        ▼                                ▼
┌─ weft-runtime  (ONE process, systemd user unit weft-runtime.service) ───────────┐
│   roles inside:  Dispatcher │ Broker │ Listener │ Supervisor                     │
│   ports: 9999 public API (+ internal routes again under /_internal)             │
│          9090 internal: /dispatcher /broker /listener /supervisor               │
│          9998 outside door                                                      │
│   local-only loops: local_alarm (wakes table)  ·  idle_workers (every 30s)      │
└───────┬──────────────────┬────────────────────────────┬─────────────────────────┘
        │ SQL              │ docker CLI                  │ S3 API
        ▼                  ▼                              ▼
  weft-postgres     docker network "weft"          weft-object-store
  (container,       ├─ weft-w-<project>-<img6>     (seaweedfs container,
   data in          │    worker, one per             port 8333, one bucket
   ~/.local/share/  │    (project, image),           per install)
   weft/postgres-   │    stopped after 300s idle
   data)            ├─ weft-long-<exec>  (--rm, one per long run)
                    └─ infra units:  wi-<node>-<hash>-<unit>            (agent)
                                     wi-<node>-<hash>-<unit>-<container> (apps)
                                     + volumes (disk | scratch)
 image builds: docker build (BuildKit), images stay in local docker
 identity: HMAC tokens signed with WEFT_IDENTITY_KEY (secrets.env)


GCP (default: WEFT_SERVERLESS_ROLES unset, WEFT_LISTENER_MACHINE unset)
═══════════════════════════════════════════════════════════════════════════════════
 you / webhooks / callers ──► static public IP (or your domain)
                                        │  :80 ACME challenges, else redirect
                                        ▼  :443 TLS (Let's Encrypt, IP + domains)
┌─ Compute Engine VM "machine" (e2-micro, COS, tag <name>-machine) ───────────────┐
│  weft-postgres.service  postgres:18-alpine, data on /mnt/disks/data             │
│  weft-runtime.service   weft-runtime serve  (--network host)                    │
│     front door ── public API ──────────────► proxied to the dispatcher service  │
│               ── /_internal/listener/... ──► the Listener, in this process      │
│     Listener   holds sockets / streams / SSE subscriptions, timers via Alarm    │
└───────▲────────────────────────────────────────────▲────────────────────────────┘
        │ 5432 (only from tag <name>-role)           │ 9090 internal (from the subnet)
┌───────┴──────────── Cloud Run, one service each, scale on own load, to zero ─────┐
│  <name>-role-dispatcher   <name>-role-broker   <name>-role-supervisor           │
│  run `serve --role X`, serve that role at their root, loops run per tick        │
│  tag <name>-role on VPC egress · invoker: core SA only, the broker: anyone      │
│  (the broker checks every caller's identity itself)                             │
└──────┬─────────────────┬───────────────────┬────────────────────┬────────────────┘
       │ Cloud Run API   │ Compute API        │ Cloud Build        │ Cloud Tasks
       ▼                 ▼                    ▼                    ▼
  worker services    infra: one VM per    builds → Artifact     queue "wakes": one task
  wk-<proj>-<img>    unit (COS, private   Registry              per wake, OIDC → a role's
  per-project SA,    IP, tag <name>-      (context in the       own service, or the
  tag <name>-worker  infra, runs          builds bucket)        machine's /_internal/<role>
  long runs:         `unit-agent host`)                         for a role on a machine
  Cloud Run job wj-…

 firewall:  <name>-infra VMs, any port, from: roles · machine · listener · workers · infra
            machine :9090 and unit agents :7979, from the whole subnet
            Postgres :5432 only from the Cloud Run roles (never workers or infra)
 files: GCS bucket "files" through the S3 API (HMAC key)
 identity: Google ID tokens (core SA = weft itself, project SA = that project)
 secrets: Secret Manager (DB URL on the machine's private IP)


GCP with WEFT_LISTENER_MACHINE=true
═══════════════════════════════════════════════════════════════════════════════════
  machine (e2-micro):  Postgres + front door only, no role
       └─ /_internal/listener/... ──► passed on to the listener's machine
  VM "<name>-listener" (e2-small, private IP only, tag <name>-listener):
       weft-listener.service  weft-runtime serve --role listener  (always up,
       its whole internal port at its root, holds the connections)
  everything else as above; the other roles reach it at its private address
```

## 1. A process's life: one binary, three shapes

```
weft-runtime serve --config <install config>
   ├─ no --role → runs every role placed "machine", each under its prefix
   │              (local: all four; GCP default: the listener + front door)
   └─ --role X  → runs that one role at its root:
                    "serverless"   on Cloud Run (GCP default: dispatcher,
                                   broker, supervisor)
                    "own_machine"  on a VM of its own (the listener with
                                   WEFT_LISTENER_MACHINE)

 opens Postgres if it runs Dispatcher or Broker (local: always)
 applies the schema (refuses a database from before the history restart)
 one Postgres LISTEN connection wakes every loop:
   task ready · exec events · dispatcher events · wakes

 machine / own_machine role: loops run for the process's whole life
 serverless role:  no loops; a guarded POST /_weft/tick drains everything once,
                   then sets its next wake through Alarm (scale-to-zero)
```

Background loops, all inside the runtime:

```
DISPATCHER
  dispatcher_picker      runs tasks targeted at the dispatcher
  delivery               sends pending runs to workers (§4)
  lifecycle_claimer      runs deactivate / reactivate / upgrade commands
  journal_bridge         exec_event rows → projection → SSE event bus
  infra_event_bridge     infra_event rows → SSE event bus
  reapers:  removed_projects 30s · stuck_transitions 30s · orphaned_live 30s
            stale_cancels 60s · entry_rate 60s · ghost_infra_leases 5m
            tasks 1h · retired_rows 1h · parked_fires & storage_sweep (on write)
BROKER      runtime_file_expiry 60s · connect_expiry 300s
SUPERVISOR  ownership · lifecycle (apply/stop/terminate) · health
LISTENER    rehydrates every signal once at boot
FRONT DOOR  (cloud only) certificates every 30s
LOCAL ONLY  local_alarm (delivers due wakes) · idle_workers (stops idle workers)
```

## 2. Who may talk to whom (the broker door)

```
                  ┌──────────── Postgres ────────────┐
                  │  only Dispatcher and Broker      │
                  │  hold a connection               │
                  └───▲──────────────────────▲───────┘
                      │                      │
               Dispatcher                  Broker  ◄── every other caller:
                                             ▲      Listener, Supervisor, Workers
                                             │
   each call carries:  Authorization: Bearer <identity token>
                       x-weft-instance: <instance id>   x-weft-role: <role>

   LOCAL: token = HMAC {principal, exp} signed with WEFT_IDENTITY_KEY
          role tokens last 1h; a worker's token is project-scoped, ~100y
   GCP:   token = Google ID token from the metadata server
          core SA → "Core" (weft)     project SA → that project

   broker checks: role, tenant, project scope, and for journal writes:
      execution.owner_instance == the calling instance  (else refused)
```

## 3. Creating a project and running it

```
 weft new                              (no network at all)
   writes weft.toml (project id = UUID), src/main.weft, nodes/base_catalog,
   git init, Tangle assistant files

 weft run
  CLI (your machine, both cases)
   1 compile locally: weft-compiler + catalog → definition, all errors first
   2 snapshot: hash every covered file (src, nodes, prompts, assets, ...)
       └─► upload only the blobs the store lacks (project's asset plane
           in the object store)
   3 POST /projects/{id}/builds   {manifest, nodeSet, assets}
   4 POST /projects/{id}/versions/runs  {manifest, hashes, seed, spec, ...}
   5 follow over SSE (/events/execution/{id}) unless --detach

  DISPATCHER
   builds ─► re-fetch files, verify hashes, recompile with the install catalog
         ├─► project_definition (definition_hash)   project_code (binary_hash)
         └─► build plan: worker image  weft-worker:<binary_hash>
                         + one infra image per infra node  weft-infra-<name>:<hash>
                image missing? → image_build ledger row (driver lease 60s,
                a sibling takes over a dead driver), compile lane = own cache
                        LOCAL: docker build FROM weft-builder-base
                        GCP:   Cloud Build → Artifact Registry
   runs ─► under the tree lock:
             project_version (id from manifest, parent = head)
             version_run     (execution_id, version, seed, spec)
             start execution (§4), then move head
```

The project's rows are `project`, `project_definition`, `project_code`, `project_version` and `version_run`. The project folder on your disk stays the source of truth for the code.

## 4. An execution: birth, delivery, worker, journal, stream

```
BIRTH (dispatcher, one transaction)
   exec_event:  ExecutionStarted (+ NodeKicked per entry)
   execution:   row (project, tenant, member, phase, owner_instance NULL)
   task:        kind=execute, execution_id, binary_hash, run_class

DELIVERY (dispatcher "delivery" loop; wakes on task-ready, 30s safety sweep)
   take_deliveries: pending (or lapsed) execute/resume tasks, not pinned,
                    no outstanding delivery → stamp delivered_until = now+120s
   short run ─► Runner.endpoint(project, image, levers)
                 LOCAL: start/reuse container weft-w-…  (127.0.0.1::8080)
                 GCP:   Cloud Run service wk-… (scales from zero)
               POST {worker}/_weft/run/<execution_id>   (held open)
               ◄── Completed | Failed | LeaseLost | NothingToRun
   long run  ─► Runner.start_long
                 LOCAL: container weft-long-<exec> running `--run <id>`
                 GCP:   Cloud Run job wj-…  (up to 7 days)

WORKER (the program's compiled binary, weft-engine)
   claim_one via broker  (FOR UPDATE SKIP LOCKED, claim 60s, heartbeat 15s)
      └─ DB trigger: execution.owner_instance = claimer   ("latest claim wins")
   fetch definition via broker → fold the journal → run ready nodes
   every event: POST broker /v1/journal/record  (owner check, see §2)
   holds a long-poll /v1/task/wait_cancels for cancels
   ends: Completed | Failed | Stalled (waiting on something, task done) | Stuck

STREAM
   exec_event rows ─NOTIFY─► journal_bridge ─► projection ─► EventBus
        ─► SSE /events/execution/{id}  /events/project/{id}  …/displays
        ─► CLI follow, VS Code extension

CRASH
   worker dies → no heartbeat → claim lapses after 60s → redelivered
   → the new claimer becomes the owner → the old one's writes are refused
```

## 5. Waiting and resuming (suspension)

```
 node calls ctx.await_signal(kind)        (a form, a timer, a wait for a value)
   ├─ already resolved in the journal? → returns the value (replay)
   └─ new: enqueue register_signal task (is_resume) ─► return Suspended

 register_signal (dispatcher task)
   listener /register  →  write `signal` row  (rolls back the listener if it fails)
   journal SuspensionRegistered

 worker: nothing else can move → Stalled → task completes → worker answers
         (the worker process stays up for other runs; idle → stopped later)

 RESOLUTION (any of):
   form submit    POST /signal/{token}          (browser ext, public page)
   timer due      Alarm → listener /wake → fire_signal task (via broker)
   weft wake      POST /executions/{id}/wake/{node} → listener wake_by_hand
        │
        ▼  dispatch_listener_outcome → listener /process → ProcessTarget::Resume
   journal SuspensionResolved → enqueue resume task ({exec}:resume) → delete signal
        │
        ▼  delivery (§4) → any worker → folds journal → node body replays
           from the top, the await now returns the value

 CANCEL  POST /executions/{id}/cancel
   journal NodeCancelled… then ExecutionCancelled  · listener unregister
   worker driving it hears it on wait_cancels → cancellation flag
```

## 6. Triggers and the listener

```
 weft activate
   TriggerSetup execution (on a worker): each trigger returns its SignalSpec
   → captured as trigger_bake rows → arming = register_signal (not resume)
     entry signals keep one stable token per (project, node)

 LISTENER keeps an in-memory registry of every armed signal (reloads misses
 from the broker). What each kind needs between two fires:

   timer / schedule / poll ─► next wake in the row's kind_state, handed to Alarm
        LOCAL: `alarm` table, local_alarm loop POSTs the due wake back
        GCP:   Cloud Tasks task → OIDC → /_internal/listener/wake
   sse / socket / stream / event source ─► the listener holds the connection
        (so the listener must stay on the machine)
   provider events (slack, …) ─► POST /events/{service}/{topic} → /match_push
   form, route, live connection ─► passive: the dispatcher hosts the URL

 A FIRE BECOMES A RUN
   listener-raised fire → fire_signal task (via broker)
   HTTP fire (/signal/{token}, POST /{mount path}, /events/…)
      → entry limits (per-minute buckets, at-once rows)
      → dispatch_listener_outcome → listener /process
          Resume → §5          Entry → route_entry task (dedup entry:{token}:{fire})
                                        execution id = v5(fire id), birth §4
      project not Active? → parked_fires, replayed later
```

## 7. Live callers (routes, WebSockets)

```
 caller ──► GET /connect/<path>
            dispatcher: match the armed route, caller gate (keys, JWT, member)
            mint caller ticket  v1.<payload>.<HMAC>  (execution id, project,
            binary hash, route, verdict, expiry)
       ◄── 307 (HTTP) or a URL (WS):  /live/<project>/<path>?wct=<ticket>
 caller ──► /live/...  → live_relay: verify ticket → Runner.endpoint(image)
            → forward everything (WS too) to that worker; weft's own
              credential goes in its own header, the caller's Authorization
              reaches your program untouched
 worker ──► re-verifies the ticket → live_arrival task (via broker)
            → execution born NOW, pinned to THIS worker instance
              (nothing is born at /connect: a caller who never arrives
               leaves nothing behind)

 LOCAL: public path via the tunnel → 9998;   GCP: via the front door
```

## 8. Infrastructure

```
 weft infra start  → POST /projects/{id}/infra/sync
   dispatcher starts an InfraSetup execution → worker runs the node's
   provision_infra(input) → InfraSpec
   worker → broker /v1/infra/enqueue_apply  → infra_lifecycle_command (Apply)
          → broker /v1/infra/wait_apply     (CLI waits on the command)

 SUPERVISOR (talks only to the broker)
   ownership: lease per project in infra_owner (a dead supervisor's lease expires)
   lifecycle: claim command → resolve spec → set_provisioning (fence)
              → InfraHost.apply_unit for units that are down or new
              → wait readiness → endpoint addresses → set_applied
   health:    observe units → flaky/recovered windows → infra_event,
              infra_node.status

 InfraHost.apply_unit, per unit:
   LOCAL                                   GCP
   ─────                                   ───
   docker network "weft"                   one Compute Engine VM per unit
   volumes: disks (kept) + scratch         persistent disks, mounted /mnt/disks
   fs_group → chown the disks,             no public IP, runs as the project SA
     every container joins the group       startup: GPU driver if needed,
   agent container (unit-agent serve):       `weft-runtime unit-agent host`
     owns the unit's network, answers         = the LOCAL host, run on the VM,
     probes, DNS name on "weft"                applied from VM metadata,
   init containers, then containers           called by the runtime signed as core
     (all share the agent's network →
      reach each other on 127.0.0.1)
   GPU: --gpus all if Docker has the        GPU: guestAccelerators (kind, count)
     nvidia runtime (checked at start)

 infra_node row per (project, node place, member):
   endpoints_json          URLs workers use
   install_endpoints_json  URLs weft's own roles use
   doors_json              SameNetwork addresses    public_paths_json

 HOW A WORKER REACHES ITS INFRA
   ctx.endpoint("api") → broker /v1/infra/endpoint_url → from endpoints_json
   Expose::Project      only the workers:  LOCAL unit's DNS name on "weft"
                                           GCP the VM's private IP (VPC)
   Expose::SameNetwork  also this machine: LOCAL loopback port
                                           GCP private network   (weft infra list-doors)
   Expose::Public       through weft:      /infra/<project>/<instance>/<path>

 display: /projects/{id}/infra/nodes/{node}/live → the unit's /live
          buttons → /action
 stop / terminate → command rows → supervisor → InfraHost (keep disks or not)
```

## 9. Tokens

```
 caller ticket    v1.<payload>.<sig>, HMAC, for /live (§7)
 signal token     wft-<six words>; only the hash stored; `weft token`
                  scopes: projects, tags, displays (opt-in)
                  opens /signal-token/signals (list, clear), files, displays
 operator key     a signal_token row of kind operator; `Bearer wft-…`
                  required on a cloud install; LOCAL: loopback = tenant "local"
 member token     wft-… for one member of one project → /member/* pages
 identity token   role / worker → broker (§2)

 BROWSER EXTENSION (holds signal tokens)
   tasks page:  GET  /signal-token/signals         (what is waiting on you)
   submit form: POST /signal/{token}  → §5 resolution
   skip:        POST /signal/{token}/skip     cancel: DELETE /signal/{token}
   popup: clear all → DELETE /signal-token/signals
 VS CODE EXTENSION: REST + SSE (/events/project, /events/execution, displays)
```

## 10. Connections, files, domains

```
 CONNECTIONS (access store, sealed AES-256-GCM)
   weft connect → /access/connect/begin → provider → /access/oauth/callback
     callback address: Google needs an install domain (https://<domain>),
     others need any https address (LOCAL: the tunnel)
   picks: which grant each access node uses, per install (never in source)
   at run time: worker → broker /v1/access/resolve (refreshes lazily)

 FILES (bytes never pass through the broker)
   worker → broker presign → PUT/GET straight to the store
     presign audience: External (public) · Internal (workers) · Runtime (broker)
     LOCAL seaweedfs          GCP GCS via S3 API
   public links /public/files/{token}; run ends → storage_sweep deletes
   what was not kept

 DOMAINS (install_domain table, `weft domain add --for install|frontend|api`)
   LOCAL: domains are refused; the tunnel is the public address
   GCP front door by Host:  IP or install domain → whole public surface
                            frontend domain → its upstream (proxy)
                            api domain → that project's routes at /
                            anything else → 421
```

That covers every piece the three read-throughs found.