import { describe, it, expect } from 'vitest';
import {
  nodeIsTrigger,
  nodeRequiresInfra,
  projectHasInfra,
  projectHasTriggers,
} from './node-roles';
import { INCLUDE_NODE_TYPE } from '../types';

/// An `@include` as the editor's parse delivers it: one opaque node, no
/// body, plus a record of what the file behind it holds.
function includeNode(contents: { requiresInfra?: boolean; hasTrigger?: boolean }) {
  return { nodeType: INCLUDE_NODE_TYPE, includePath: 'lib/thing.weft', includeContents: contents };
}

describe('the project-level questions count through includes', () => {
  it('finds a trigger that only exists inside an included file', () => {
    const nodes = [{ nodeType: 'Text' }, includeNode({ hasTrigger: true })];
    expect(projectHasTriggers(nodes)).toBe(true);
    // This is the bug it fixes: the graph shows one plain node and one
    // opaque box, so counting per-node says the project has no trigger
    // and the Activate button never appears.
    expect(nodes.some(nodeIsTrigger)).toBe(false);
  });

  it('finds an infra node that only exists inside an included file', () => {
    const nodes = [{ nodeType: 'Text' }, includeNode({ requiresInfra: true })];
    expect(projectHasInfra(nodes)).toBe(true);
    expect(nodes.some(nodeRequiresInfra)).toBe(false);
  });

  it('says no when the included file holds neither', () => {
    const nodes = [{ nodeType: 'Text' }, includeNode({})];
    expect(projectHasTriggers(nodes)).toBe(false);
    expect(projectHasInfra(nodes)).toBe(false);
  });

  it('still counts a visible node, include or no include', () => {
    const nodes = [{ nodeType: 'Route', features: { isTrigger: true } }];
    expect(projectHasTriggers(nodes)).toBe(true);
    const infra = [{ nodeType: 'PostgresDatabase', requiresInfra: true }];
    expect(projectHasInfra(infra)).toBe(true);
  });

  it('is false on an empty graph', () => {
    expect(projectHasTriggers([])).toBe(false);
    expect(projectHasInfra([])).toBe(false);
  });
});
