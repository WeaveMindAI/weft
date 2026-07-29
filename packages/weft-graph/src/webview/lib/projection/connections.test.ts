// Layer 3: two config writes on WIRED nodes through the real projection
// path, together. One value is the object-valued access handle, the
// other a picked resource's bare id string; they sit on DIFFERENT nodes
// wired to each other, so a regression shows up as one write clobbering
// the other: pick a file and the account clears, pick an account and
// the file clears. Both directions are pinned here.
import { describe, it, expect } from 'vitest';
import type { ProjectDefinition, NodeInstance } from '../types';
import { applyOpsToProject, type ProjectionCatalog } from './apply';
import { diffConfigOps } from './config-diff';

const catalog: ProjectionCatalog = {
  GoogleAccess: {
    defaultInputs: [],
    defaultOutputs: [{ name: 'access', portType: 'Access', required: true }],
  },
  GoogleSheetsRead: {
    defaultInputs: [
      { name: 'account', portType: 'Access', required: false },
      { name: 'spreadsheet', portType: 'String', required: true },
    ],
    defaultOutputs: [{ name: 'rows', portType: 'List[JsonDict]', required: true }],
  },
};

function node(partial: Partial<NodeInstance> & { id: string; nodeType: string }): NodeInstance {
  return {
    label: null,
    config: {},
    position: { x: 0, y: 0 },
    inputs: [],
    outputs: [],
    features: {},
    scope: [],
    ...partial,
  };
}

/// The graph the bug was reported on: an access node feeding a reader.
function project(): ProjectDefinition {
  return {
    id: 'p',
    name: 'access',
    description: '',
    nodes: [
      node({ id: 'google_access_1', nodeType: 'GoogleAccess' }),
      node({ id: 'google_sheets_read_1', nodeType: 'GoogleSheetsRead' }),
    ],
    edges: [
      {
        id: 'e1',
        source: 'google_access_1',
        sourceHandle: 'access',
        target: 'google_sheets_read_1',
        targetHandle: 'account',
      },
    ],
    groups: [],
  } as ProjectDefinition;
}

const CONNECTION = { id: 'grant-1', identity: 'admin@weavemind.ai' };
// The stored shape of a picked resource is the BARE id string (the
// field's declared type is String; the label is display-only cache).
const SHEET = '1-m4cSe';

function configOf(p: ProjectDefinition, nodeId: string): Record<string, unknown> {
  return (p.nodes.find((n) => n.id === nodeId)?.config ?? {}) as Record<string, unknown>;
}

describe('config writes on two wired nodes never clobber each other', () => {
  it('an object config value round-trips through the projection', () => {
    const ops = diffConfigOps('google_access_1', { account: CONNECTION }, {}, false);
    expect(ops).toHaveLength(1);
    const after = applyOpsToProject(project(), ops, catalog);
    expect(configOf(after, 'google_access_1').account).toEqual(CONNECTION);
  });

  it('picking a resource on one node keeps the connection on the other', () => {
    // Connect the account, then pick a file (each write spreads the
    // WRITING node's own config, which is what the editor does).
    let p = applyOpsToProject(
      project(),
      diffConfigOps('google_access_1', { account: CONNECTION }, {}, false),
      catalog,
    );
    p = applyOpsToProject(
      p,
      diffConfigOps('google_sheets_read_1', { spreadsheet: SHEET }, {}, false),
      catalog,
    );
    expect(configOf(p, 'google_sheets_read_1').spreadsheet).toEqual(SHEET);
    expect(
      configOf(p, 'google_access_1').account,
      'picking a file must not clear the wired account',
    ).toEqual(CONNECTION);
  });

  it('picking a connection keeps the resource already picked', () => {
    // The mirror direction: the file first, then (re)picking the account.
    let p = applyOpsToProject(
      project(),
      diffConfigOps('google_sheets_read_1', { spreadsheet: SHEET }, {}, false),
      catalog,
    );
    p = applyOpsToProject(
      p,
      diffConfigOps('google_access_1', { account: CONNECTION }, {}, false),
      catalog,
    );
    expect(configOf(p, 'google_access_1').account).toEqual(CONNECTION);
    expect(
      configOf(p, 'google_sheets_read_1').spreadsheet,
      'picking an account must not clear the picked file',
    ).toEqual(SHEET);
  });

  it('re-picking the SAME connection emits no op (no phantom rewrite)', () => {
    // The reported reconnect gesture: choosing the connection already
    // set. An op here would rewrite source for a no-change gesture.
    const ops = diffConfigOps(
      'google_access_1',
      { account: { ...CONNECTION } },
      { account: CONNECTION },
      false,
    );
    expect(ops).toEqual([]);
  });
});
