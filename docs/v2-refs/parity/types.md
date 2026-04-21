# Types Parity

**v1 source**: `dashboard-v1/src/lib/types/index.ts`.

## Core shapes the v2 extension needs to mirror

### PortDefinition (line 346-359)

```ts
{
  name: string;
  portType: string;               // 'String', 'List[T]', 'Dict[K,V]', 'T__scoped', 'MustOverride', 'Image | Video | ...'
  required: boolean;
  description?: string;
  laneMode?: 'Single' | 'Gather' | 'Expand';
  laneDepth?: number;             // default 1
  configurable?: boolean;         // default true unless type is non-configurable
}
```

v2 has this already in the protocol but as REQUIRED `laneMode`
and `laneDepth` (from the Rust compiler). v1's optional flags
are compatible.

### NodeInstance (line 582-599)

v2 calls this `NodeDefinition`. Matches exactly except:
- v1: `parentId?: string` at top level.
- v2: derived from `scope: string[]`; `parentId` lives in `config.parentId`.

Both carry `scope`, `groupBoundary`, `inputs`, `outputs`, `features`,
`position`, `config`, `sourceLine`.

### NodeFeatures (line 470-493)

```ts
{
  isTrigger?: boolean;
  triggerCategory?: 'Webhook' | 'Polling' | 'Schedule' | 'Socket' | 'Local' | 'Manual';
  runLocationConstraint?: 'local' | 'cloud' | 'any';
  canAddInputPorts?: boolean;
  canAddOutputPorts?: boolean;
  hidden?: boolean;                  // hide from command palette
  showRunLocationSelector?: boolean;
  showDebugPreview?: boolean;
  isInfrastructure?: boolean;
  hasFormSchema?: boolean;
  infrastructureSpec?: InfrastructureSpec;
  hasLiveData?: boolean;
  oneOfRequired?: string[][];
}
```

### FieldDefinition (line 378-394)

```ts
{
  key: string;
  label: string;
  type: 'text' | 'textarea' | 'code' | 'select' | 'multiselect'
      | 'number' | 'checkbox' | 'password' | 'blob' | 'api_key' | 'form_builder';
  placeholder?: string;
  options?: string[];
  defaultValue?: unknown;
  description?: string;
  accept?: string;          // blob
  provider?: ApiKeyProvider;
  min?: number; max?: number; step?: number;  // number
  maxLength?: number; minLength?: number;     // text/textarea
  pattern?: string;         // text
}
```

My v2 protocol has a subset (`kind`, `description`, loose
`[key]: unknown` for extras). I'll extend it to match this
full shape before porting FieldEditor properly.

### NodeExecution (line 441-457)

```ts
{
  id: string;
  nodeId: string;
  status: 'running' | 'completed' | 'failed' | 'waiting_for_input' | 'skipped' | 'cancelled';
  pulseIdsAbsorbed: string[];
  pulseId: string;
  error?: string;
  callbackId?: string;
  startedAt: number;
  completedAt?: number;
  input?: unknown;
  output?: unknown;
  costUsd: number;
  logs: unknown[];
  color: string;
  lane: Array<{ count: number; index: number }>;
}
```

My v2 `NodeExec` subset doesn't carry pulseIds, costUsd, logs,
color, lane. Need to wire the full shape from dispatcher SSE to
webview. `execution.md` tracks this.

### LiveDataItem (line 463-468)

```ts
{
  type: 'text' | 'image' | 'progress';
  label: string;
  data: string | number;   // text: string; image: data URI; progress: 0..1
}
```

Not yet in v2 protocol. Add when wiring live data rendering.

### WeftType parser

Recursive type parser (from `weft-type.ts` in `utils/`). Already
ported to v2 (`extension-vscode/src/webview/utils/weft-type.ts`).

### FileRef (line 370-376)

Blob file reference:
```ts
{
  file_id?: string;   // cloud-managed
  url: string;
  filename: string;
  mime_type: string;
  size_bytes: number;
}
```

Not in v2 protocol; deferred until blob upload lands (phase B).

## Enums and constants to port

- `ALL_PRIMITIVE_TYPES`: ported.
- `MEDIA_TYPES`: ported.
- `NodeCategory`: `'Triggers' | 'AI' | 'Data' | 'Flow' | 'Utility' | 'Debug' | 'Infrastructure'`. Add to protocol for CommandPalette.
- `NodeExecutionStatus`: `'running' | 'completed' | 'failed' | 'waiting_for_input' | 'skipped' | 'cancelled'`. Add to v2 NodeExec.

## What's different in v2's shape

- **ProjectDefinition.groups** (new): pre-flatten group tree.
- **NodeDefinition.groupBoundary** (new): passthrough role tag.
- **NodeDefinition.scope** as required `string[]` (was optional in v1).
- **label** stays a top-level NodeDefinition field, NOT in config.
  (v1 moves it in/out; v2 keeps it canonical.)
- **configSpans** on NodeDefinition: exists but empty until the
  compiler populates. Without it, surgical updates rewrite whole
  node.
