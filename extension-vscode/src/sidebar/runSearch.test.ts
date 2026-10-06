// The runs view's search reads as the listing's filters: each
// `name:value` one filter, every other word searched for in what the run
// recorded.

import { describe, expect, it } from 'vitest';
import { parseRunSearch } from './runSearch';

describe('parseRunSearch', () => {
  it('reads every filter and leaves the rest as words', () => {
    expect(parseRunSearch('status:failed route:notes.list.door node:hash instance:wa tag:vip since:2h ada@example.com', 10_000)).toEqual({
      params: {
        status: 'failed',
        entry_node: 'notes.list.door',
        node: 'hash',
        instance: 'wa',
        tag: 'vip',
        started_after: String(10_000 - 7200),
        search: 'ada@example.com',
      },
    });
  });

  it('keeps a quoted phrase whole, a colon inside it included', () => {
    expect(parseRunSearch('"payment declined: card" order 42', 0)).toEqual({ params: { search: '"payment declined: card" order 42' } });
  });

  it('reads an address with a colon as a word', () => {
    expect(parseRunSearch('https://example.com/x', 0)).toEqual({ params: { search: 'https://example.com/x' } });
  });

  it('says what does not read', () => {
    expect(parseRunSearch('status:broken', 0)).toEqual({
      error: 'status:broken is not a status; one of running, waiting_for_input, completed, failed, cancelled',
    });
    expect(parseRunSearch('since:yesterday', 0)).toEqual({ error: 'since:yesterday is not a time; write it like 30s, 10m, 2h or 3d' });
  });

  it('is nothing at all when empty', () => {
    expect(parseRunSearch('   ', 0)).toEqual({ params: {} });
  });
});
