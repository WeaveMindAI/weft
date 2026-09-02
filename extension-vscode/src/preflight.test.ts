import { describe, expect, it } from 'vitest';
import { preflightBlock, PREFLIGHT_VERBS } from './preflight';
import type { Diagnostic as WeftDiagnostic } from '../../packages/weft-graph/src/protocol';

function diag(over: Partial<WeftDiagnostic>): WeftDiagnostic {
  return {
    line: 3,
    column: 2,
    endLine: 0,
    endColumn: 0,
    severity: 'error',
    message: "OpenRouterProvider 'llm' has no OpenRouter connection picked; connect one on the node.",
    code: 'rule-runtime',
    ...over,
  };
}

describe('preflightBlock', () => {
  it('lets a clean project through', () => {
    expect(preflightBlock([], '/p/main.weft')).toBeNull();
  });

  it('lets warnings through without blocking', () => {
    expect(preflightBlock([diag({ severity: 'warning' })], '/p/main.weft')).toBeNull();
  });

  it('blocks on one error with the finding as the banner text', () => {
    const block = preflightBlock([diag({})], '/p/main.weft');
    expect(block).not.toBeNull();
    expect(block!.message).toContain('no OpenRouter connection picked');
    expect(block!.details.stage).toBe('preflight');
    expect(block!.details.diagnostics).toHaveLength(1);
    expect(block!.details.diagnostics[0].location).toEqual({
      file: '/p/main.weft',
      line: 3,
      column: 2,
    });
  });

  it('blocks on several errors with a count summary, listing every finding', () => {
    const block = preflightBlock(
      [
        diag({ message: 'a needs a connection' }),
        diag({ message: 'b needs a model', line: 9 }),
        diag({ severity: 'warning', message: 'just a warning' }),
      ],
      '/p/main.weft',
    );
    expect(block!.message).toContain('2');
    // The complete list rides along, warnings included.
    expect(block!.details.diagnostics.map((d) => d.severity)).toEqual([
      'error',
      'error',
      'warning',
    ]);
  });

  it('maps hint severity onto info (the modal has no hint rung)', () => {
    const block = preflightBlock(
      [diag({}), diag({ severity: 'hint', message: 'consider x' })],
      '/p/main.weft',
    );
    expect(block!.details.diagnostics[1].severity).toBe('info');
  });

  it("routes a finding to its own file, and a file-less one to the entry", () => {
    const block = preflightBlock(
      [diag({ file: '/p/greeter.weft' }), diag({ message: 'entry-side' })],
      '/p/main.weft',
    );
    expect(block!.details.diagnostics[0].location?.file).toBe('/p/greeter.weft');
    expect(block!.details.diagnostics[1].location?.file).toBe('/p/main.weft');
  });

  it('gates exactly the shipping verbs', () => {
    expect([...PREFLIGHT_VERBS].sort()).toEqual(['activate', 'resync', 'run']);
    expect(PREFLIGHT_VERBS.has('build')).toBe(false);
  });
});
