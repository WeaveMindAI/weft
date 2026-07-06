// THE one snake_case -> camelCase remap for the dispatcher's
// `/projects/{id}/status` payload (identical whether it arrives via
// `weft status --json` on the CLI or a direct HTTP fetch). Every host
// (the VS Code extension, a browser-based editor) builds its
// `ActionAvailability` snapshot AND its `BackendSnapshot` through
// here, so the mapping can never fork between them.

import type {
  ActionAvailability,
  ActionVerb,
  BackendSnapshot,
  ProjectTransition,
} from './protocol';

/// The raw dispatcher status payload (snake_case wire shape). Only the
/// fields the frontends consume; extra fields are ignored.
// SYNC: RawStatusPayload <-> crates/weft-dispatcher/src/api/project.rs ProjectStatusResponse
export interface RawStatusPayload {
  status?: string;
  transition?: string;
  mode?: string;
  fires_deadline_unix?: number;
  running_count?: number;
  orphaned_infra?: boolean;
  infra_rollup?: string;
  infra?: Array<{
    node_id?: string;
    node_type?: string;
    status?: string;
    failureStage?: string;
    failureMessage?: string;
  }>;
  drift?: {
    binary_drift?: boolean;
    definition_drift?: boolean;
    infra_drift?: boolean;
  };
  available_actions?: string[];
  preservation?: { parked?: number; suspended?: number };
  executions?: { last_status?: string; last_color?: string };
}

// SYNC: infra_rollup values <-> crates/weft-dispatcher/src/api/project.rs (infra_rollup)
const VALID_ROLLUPS = [
  'none',
  'stopped',
  'partial',
  'running',
  'failed',
  'flaky',
  'stopping',
  'terminating',
  'provisioning',
] as const;

const VALID_STATUSES = [
  'registered',
  'activating',
  'active',
  'deactivating',
  'inactive',
] as const;

// SYNC: VALID_TRANSITIONS <-> crates/weft-dispatcher/src/project_store.rs ProjectTransition, packages/weft-graph/src/protocol.ts ProjectTransition, crates/weft-dispatcher/src/api/project.rs ProjectStatusResponse.transition
const VALID_TRANSITIONS: ProjectTransition[] = ['none', 'building', 'cancelling_build'];

/// The honest snapshot for a project the dispatcher doesn't know yet
/// (first graph open, post-wipe): empty verb list (starter verbs light
/// from graph shape), everything at rest. Hosts return this instead of
/// faking a verb list when the status fetch 404s.
export function emptyActionAvailability(): ActionAvailability {
  return {
    availableActions: [],
    binaryDrift: false,
    definitionDrift: false,
    infraDrift: false,
    projectStatus: 'unknown',
    transition: 'none',
    orphanedInfra: false,
    mode: 'unknown',
    runningCount: 0,
    infraRollup: 'none',
    infraNodes: [],
    preservation: { parked: 0, suspended: 0 },
  };
}

/// Parse a raw status payload into the shared `ActionAvailability`.
/// Unknown enum strings collapse to their resting value rather than
/// crashing the bar (a version-skewed dispatcher shouldn't brick the
/// UI); the raw string is still visible via a status refetch once the
/// client updates.
export function parseStatusPayload(raw: RawStatusPayload): ActionAvailability {
  const drift = raw.drift ?? {};
  const statusStr = String(raw.status ?? 'unknown');
  const projectStatus = (VALID_STATUSES as readonly string[]).includes(statusStr)
    ? (statusStr as ActionAvailability['projectStatus'])
    : 'unknown';
  const transitionStr = String(raw.transition ?? 'none');
  const transition: ProjectTransition = VALID_TRANSITIONS.includes(
    transitionStr as ProjectTransition,
  )
    ? (transitionStr as ProjectTransition)
    : 'none';
  const rollupStr = String(raw.infra_rollup ?? 'none');
  const infraRollup = (VALID_ROLLUPS as readonly string[]).includes(rollupStr)
    ? (rollupStr as ActionAvailability['infraRollup'])
    : 'none';
  const firesDeadlineUnix =
    typeof raw.fires_deadline_unix === 'number' ? raw.fires_deadline_unix : undefined;
  const infraNodes = (Array.isArray(raw.infra) ? raw.infra : []).map((n) => ({
    nodeId: n.node_id ?? '',
    nodeType: n.node_type ?? '',
    status: n.status ?? 'unknown',
    ...(n.failureStage !== undefined ? { failureStage: n.failureStage } : {}),
    ...(n.failureMessage !== undefined ? { failureMessage: n.failureMessage } : {}),
  }));
  return {
    availableActions: (Array.isArray(raw.available_actions)
      ? raw.available_actions
      : []) as ActionVerb[],
    binaryDrift: !!drift.binary_drift,
    definitionDrift: !!drift.definition_drift,
    infraDrift: !!drift.infra_drift,
    projectStatus,
    transition,
    orphanedInfra: !!raw.orphaned_infra,
    mode: String(raw.mode ?? 'unknown'),
    ...(firesDeadlineUnix !== undefined ? { firesDeadlineUnix } : {}),
    runningCount: Number(raw.running_count ?? 0),
    infraRollup,
    infraNodes,
    preservation: {
      parked: Number(raw.preservation?.parked ?? 0),
      suspended: Number(raw.preservation?.suspended ?? 0),
    },
  };
}

/// Project the snapshot down to the action bar's `BackendSnapshot`.
/// One derivation so the two shapes can't drift apart per host.
export function backendFromSnapshot(snapshot: ActionAvailability): BackendSnapshot {
  return {
    available: snapshot.availableActions,
    status: snapshot.projectStatus,
    transition: snapshot.transition,
    orphanedInfra: snapshot.orphanedInfra,
    mode: snapshot.mode,
    infraRollup: snapshot.infraRollup,
    runningCount: snapshot.runningCount,
    ...(snapshot.firesDeadlineUnix !== undefined
      ? { firesDeadlineUnix: snapshot.firesDeadlineUnix }
      : {}),
  };
}
