// What a person types in the runs view's search, read as the listing's
// filters (`GET /executions`). Each `name:value` narrows by one thing a
// run has, and every other word is searched for in what the finished run
// recorded (the trigger's input, what each node sent on, an error, a
// log line), the way `weft executions --search` does:
//
//   status:failed route:notes.list.door since:2h ada@example.com
//
// - `status:` running, waiting_for_input, completed, failed, cancelled
// - `route:` (or `trigger:`) the node that started the run
// - `node:` a node that fired in the run, wherever it sits
// - `instance:` the instance the run was for
// - `tag:` a tag the run carries
// - `since:` how long ago at most: 30s, 10m, 2h, 3d
//
// A phrase in quotes is searched as written.

/** The listing's query parameters a search stands for, or why it does not read. */
export type RunSearch = { params: Record<string, string> } | { error: string };

// SYNC: STATUSES <-> crates/weft-core/src/program.rs RunStatus, crates/weft-dispatcher/src/journal/postgres.rs list_executions (status clause)
const STATUSES = ['running', 'waiting_for_input', 'completed', 'failed', 'cancelled'];

const UNITS: Record<string, number> = { s: 1, m: 60, h: 3600, d: 86400 };

/** Read `text` at `nowSecs` (unix seconds). */
export function parseRunSearch(text: string, nowSecs: number): RunSearch {
  const params: Record<string, string> = {};
  const words: string[] = [];
  // A quoted phrase is one token, quotes kept for the words search.
  for (const token of text.match(/"[^"]*"|\S+/g) ?? []) {
    const named = /^([a-z_]+):(.+)$/.exec(token);
    if (!named || token.startsWith('"')) {
      words.push(token);
      continue;
    }
    const [, name, value] = named;
    switch (name) {
      case 'status':
        if (!STATUSES.includes(value)) {
          return { error: `status:${value} is not a status; one of ${STATUSES.join(', ')}` };
        }
        params.status = value;
        break;
      case 'route':
      case 'trigger':
        params.entry_node = value;
        break;
      case 'node':
        params.node = value;
        break;
      case 'instance':
        params.instance = value;
        break;
      case 'tag':
        params.tag = value;
        break;
      case 'since': {
        const ago = /^(\d+)([smhd])$/.exec(value);
        if (!ago) return { error: `since:${value} is not a time; write it like 30s, 10m, 2h or 3d` };
        params.started_after = String(nowSecs - Number(ago[1]) * UNITS[ago[2]]);
        break;
      }
      default:
        // An address with a colon in it (`http://...`) is a word, not a filter.
        words.push(token);
    }
  }
  if (words.length) params.search = words.join(' ');
  return { params };
}
