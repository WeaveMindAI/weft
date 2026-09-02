import { describe, it, expect } from 'vitest';
import { translateProject } from './host-bridge';
import type { ProjectDefinition as HostProject } from '../protocol';

/** The regression this file pins: `translateProject` maps wire nodes
 *  FIELD BY FIELD, so any definition field it forgets is silently
 *  stripped from the editor's truth. `portLiterals` was dropped this
 *  way: a port field's value rendered until the first parse round-trip
 *  replaced the truth, then vanished from the graph while the source
 *  still carried it. */
describe('translateProject', () => {
  it('carries portLiterals and portLiteralSpans through the translation', () => {
    const host = {
      id: 'p1',
      nodes: [
        {
          id: 'orc',
          nodeType: 'OpenRouterConfig',
          label: null,
          config: {},
          position: { x: 0, y: 0 },
          scope: [],
          groupBoundary: null,
          inputs: [
            {
              name: 'systemPrompt', portType: 'String', required: false,
              exposure: 'all', widget: { kind: 'textarea' }, default: 'be nice',
              label: 'System prompt', placeholder: 'You are...',
              declaredType: 'String',
              fromSpec: true, requiresScopes: ['chat:write'], requiresValues: { team: 'x' },
            },
          ],
          outputs: [
            { name: 'haiku', portType: 'String', required: false, declaredType: 'String' },
            { name: 'response', portType: 'Number', required: false },
          ],
          features: {},
          portLiterals: { systemPrompt: 'test' },
          portLiteralSpans: {
            systemPrompt: {
              span: { startLine: 7, startColumn: 2, endLine: 7, endColumn: 44 },
              origin: 'inline' as const,
            },
          },
        },
      ],
      edges: [],
      groups: [],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const node = v1.nodes.find((n) => n.id === 'orc')!;
    expect(node.portLiterals).toEqual({ systemPrompt: 'test' });
    expect(node.portLiteralSpans?.systemPrompt?.origin).toBe('inline');
    // The input's resolved editor surface survives too (the field
    // renderer + form toggle read all of it off the instance).
    expect(node.inputs[0].exposure).toBe('all');
    expect(node.inputs[0].widget).toEqual({ kind: 'textarea' });
    expect(node.inputs[0].default).toBe('be nice');
    expect(node.inputs[0].label).toBe('System prompt');
    expect(node.inputs[0].placeholder).toBe('You are...');
    // `declaredType` survives on both sides. When the translation
    // dropped it, every parse round-trip un-declared every header
    // port, and the next ports gesture rewrote the header without
    // them: custom ports silently vanished from the source.
    expect(node.inputs[0].declaredType).toBe('String');
    expect(node.outputs[0].declaredType).toBe('String');
    expect(node.outputs[1].declaredType).toBeUndefined();
    // The permission-shortfall fields survive too (the same old copy
    // dropped them, deadening the shortfall check on parsed nodes).
    expect(node.inputs[0].fromSpec).toBe(true);
    expect(node.inputs[0].requiresScopes).toEqual(['chat:write']);
    expect(node.inputs[0].requiresValues).toEqual({ team: 'x' });
    // Every port is a FRESH object: the projection mutates the graph in
    // place and must never alias the parse message.
    expect(node.inputs[0]).not.toBe(host.nodes[0].inputs[0]);
  });

  it('labels a group by its local id segment, never the full dotted path', () => {
    const group = (id: string, parentGroupId: string | null) => ({
      id,
      kind: 'group' as const,
      label: null,
      inPorts: [],
      outPorts: [],
      oneOfRequired: [],
      parentGroupId,
      childGroupIds: [],
      nodeIds: [],
    });
    const host = {
      id: 'p1',
      nodes: [],
      edges: [],
      groups: [group('outer', null), group('outer.inner', 'outer')],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const labels = new Map(v1.nodes.map((n) => [n.id, n.label]));
    expect(labels.get('outer')).toBe('outer');
    expect(labels.get('outer.inner')).toBe('inner');
  });

  it("carries a group's `_should_flow` literal across as a port literal", () => {
    const host = {
      id: 'p1',
      nodes: [],
      edges: [],
      groups: [
        {
          id: 'off', kind: 'group' as const, label: null,
          inPorts: [], outPorts: [], oneOfRequired: [],
          parentGroupId: null, childGroupIds: [], nodeIds: [],
          portLiterals: { _should_flow: false },
        },
        {
          id: 'on', kind: 'group' as const, label: null,
          inPorts: [], outPorts: [], oneOfRequired: [],
          parentGroupId: null, childGroupIds: [], nodeIds: [],
        },
      ],
    } as unknown as HostProject;

    const v1 = translateProject(host, 'src', '');
    const byId = new Map(v1.nodes.map((n) => [n.id, n]));
    expect(byId.get('off')?.portLiterals).toEqual({ _should_flow: false });
    expect(byId.get('on')?.portLiterals).toBeUndefined();
  });
});
