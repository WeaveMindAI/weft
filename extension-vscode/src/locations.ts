// Where a finding or a click lands: the one canonical form of a file
// path, and the one weft -> VS Code position conversion. Shared by the
// source-jump, the Problems panel, and the tab scanning, so no two
// consumers can resolve or clamp differently.

import * as fs from 'node:fs';
import * as vscode from 'vscode';

/// The one canonical form of a file path, shared by every comparison
/// between a workspace path (possibly reached through a symlink) and a
/// compiler-emitted path (always canonicalized). A path that cannot be
/// resolved (deleted, permission) is compared as written.
///
/// Deliberately NOT memoized: a symlink can be re-pointed while the
/// window is open (a `git checkout`, a build dir's `current` link),
/// and a cached stale answer would silently route squiggles at a dead
/// path. One realpath syscall per call is cheap enough to pay.
export function canonicalPath(fsPath: string): string {
  try {
    return fs.realpathSync(fsPath);
  } catch {
    return fsPath;
  }
}

/// A weft diagnostic position as a vscode.Position: weft lines are
/// 1-based and columns 0-based character offsets, vscode is 0-based
/// both, and either can be a degenerate zero. The ONE conversion, so
/// the source-jump and the Problems panel cannot clamp differently.
export function weftPositionToVsCode(line: number, column: number): vscode.Position {
  return new vscode.Position(Math.max(0, line - 1), Math.max(0, column));
}
