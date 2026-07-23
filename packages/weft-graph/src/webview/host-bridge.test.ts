import { describe, it, expect } from 'vitest';
import { translateProject } from './host-bridge';
import type { ProjectDefinition as HostProject } from '../shared/protocol';

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
            },
          ],
          outputs: [],
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
  });
});
