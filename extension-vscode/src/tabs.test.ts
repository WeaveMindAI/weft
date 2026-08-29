// isReviewDoc decides whether a focused .weft file is being READ (a
// diff, a source-control revision) or WORKED ON, which is what decides
// whether the graph may pop over it. Only the ACTIVE tab decides; the
// tab is handed in as a plain object so the rules are pinned without
// an editor process.

import { describe, it, expect } from 'vitest';
import * as vscode from 'vscode';
import { isReviewDoc } from './tabs';

const uri = (s: string) => ({ scheme: s.split(':')[0], toString: () => s });
const doc = (s: string) => ({ uri: uri(s) }) as never;
const diffTab = (original: string, modified: string) =>
  ({ input: new vscode.TabInputTextDiff(uri(original), uri(modified)) }) as never;
const plainTab = (path: string) => ({ input: { uri: uri(path) } }) as never;

describe('isReviewDoc', () => {
  it('treats any source-control revision as review', () => {
    expect(isReviewDoc(doc('git:/p/main.weft'), undefined)).toBe(true);
  });

  it('an unsaved untitled buffer is being written, not reviewed', () => {
    expect(isReviewDoc(doc('untitled:Untitled-1'), plainTab('untitled:Untitled-1'))).toBe(false);
  });

  it('treats the working-tree side of an ACTIVE diff as review', () => {
    const active = diffTab('git:/p/main.weft', 'file:///p/main.weft');
    expect(isReviewDoc(doc('file:///p/main.weft'), active)).toBe(true);
  });

  it('a plain active tab is not review even with a diff of the same file open elsewhere', () => {
    // The background diff must not veto the editor tab the user just
    // clicked, or the graph stops following until the diff is closed.
    expect(isReviewDoc(doc('file:///p/main.weft'), plainTab('file:///p/main.weft'))).toBe(false);
  });

  it('a plain open file is not review', () => {
    expect(isReviewDoc(doc('file:///p/main.weft'), plainTab('file:///p/main.weft'))).toBe(false);
  });

  it('an active diff of a DIFFERENT file does not mark this one', () => {
    const active = diffTab('git:/p/other.weft', 'file:///p/other.weft');
    expect(isReviewDoc(doc('file:///p/main.weft'), active)).toBe(false);
  });
});
