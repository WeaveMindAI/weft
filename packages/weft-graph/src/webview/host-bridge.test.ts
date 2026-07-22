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
            { name: 'systemPrompt', portType: 'String', required: false, literal: 'anywhere' },
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
    // The port's literal placement survives too (the marker/toggle reads it).
    expect(node.inputs[0].literal).toBe('anywhere');
  });
});
