import { describe, it, expect } from 'vitest';
import { mergeSimplifiedEdges, type SimplifiedLookup } from './simplified-edges';
import type { Edge } from './types';
import {
  SIMPLIFIED_IN_HANDLE,
  SIMPLIFIED_OUT_HANDLE,
  SIMPLIFIED_INNER_SOURCE_HANDLE,
  SIMPLIFIED_LOOP_INDEX_HANDLE,
} from './constants/simplified-view';

const wire = (source: string, sourceHandle: string, target: string, targetHandle: string): Edge => ({
  id: `e-${source}-${sourceHandle}-${target}-${targetHandle}`,
  source, target, sourceHandle, targetHandle,
});

const types: Record<string, string> = { results: 'String', count: 'Number' };
const lookup = (loops: string[] = []): SimplifiedLookup => ({
  isLoop: (id) => loops.includes(id),
  sourceType: (e) => types[(e.sourceHandle ?? '').replace(/__inner$/, '')] ?? 'Any',
});

describe('mergeSimplifiedEdges', () => {
  it('merges every data wire between the same two nodes into one, keeping each type', () => {
    const merged = mergeSimplifiedEdges([
      wire('search', 'results', 'llm', 'prompt'),
      wire('search', 'count', 'llm', 'limit'),
    ], lookup());
    expect(merged).toHaveLength(1);
    expect(merged[0]).toMatchObject({ source: 'search', target: 'llm', sourceHandle: SIMPLIFIED_OUT_HANDLE, targetHandle: SIMPLIFIED_IN_HANDLE });
    expect([...merged[0].types].sort()).toEqual(['Number', 'String']);
  });

  it('keeps wires to different places apart', () => {
    const merged = mergeSimplifiedEdges([
      wire('search', 'results', 'tools', 'search'),
      wire('search', 'results', 'route', 'search'),
    ], lookup());
    expect(merged.map((m) => m.target).sort()).toEqual(['route', 'tools']);
  });

  it('routes a container\'s own inputs and a loop\'s index to their inner dots', () => {
    const merged = mergeSimplifiedEdges([
      wire('g', 'results__inner', 'g.m', 'in'),
      wire('l', 'index__inner', 'l.m', 'in'),
      wire('g2', 'index__inner', 'g2.m', 'in'),
    ], lookup(['l']));
    const dot = (source: string) => merged.find((m) => m.source === source)!.sourceHandle;
    expect(dot('g')).toBe(SIMPLIFIED_INNER_SOURCE_HANDLE);
    expect(dot('l')).toBe(SIMPLIFIED_LOOP_INDEX_HANDLE);
    // A plain group may name a port `index`: it is no loop dot there.
    expect(dot('g2')).toBe(SIMPLIFIED_INNER_SOURCE_HANDLE);
  });
});
