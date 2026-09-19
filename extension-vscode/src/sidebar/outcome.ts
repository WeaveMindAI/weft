// How a finished run is shown in the two sidebars (the executions list
// and the version tree's runs): the words beside its name and the icon
// in front of it, from its status, its cancel cause and its skip count.
// One place, so a cancelled run reads the same in both trees.

import * as vscode from 'vscode';
import type { CancelCause } from '../../../packages/weft-graph/src/protocol';

/// The words a row shows beside its name: the status, and for a
/// cancelled run who ended it, for a completed one how many firings
/// it skipped.
export function describeOutcome(
  status: string,
  cancelCause: CancelCause | null | undefined,
  skippedNodes: number | undefined,
): string {
  switch (status.toLowerCase()) {
    case 'completed':
      return skippedNodes ? `completed  ·  ${skippedNodes} skipped` : 'completed';
    case 'cancelled':
      switch (cancelCause?.kind) {
        case 'user':
          return 'stopped';
        case 'caller_gone':
          return 'cancelled  ·  caller left';
        case 'execution':
          return `cancelled  ·  stopped by run ${cancelCause.by.slice(0, 8)} (tag ${cancelCause.tag})`;
        case 'runtime':
          return `cancelled  ·  ${cancelCause.detail}`;
        default:
          return 'cancelled';
      }
    default:
      return status;
  }
}

/// The icon a row draws: a spinner while it runs, green for done,
/// red for failed, a plain stop sign for a run a person stopped, and
/// an orange crossed circle for one the runtime ended on its own (the
/// caller left, a pod went down): after the drainers stopped cancelling
/// answered runs, each of those is rare and worth a look.
export function statusThemeIcon(status: string, cancelCause?: CancelCause | null): vscode.ThemeIcon {
  switch (status.toLowerCase()) {
    case 'running':
      return new vscode.ThemeIcon('sync~spin', new vscode.ThemeColor('charts.blue'));
    case 'completed':
      return new vscode.ThemeIcon('check', new vscode.ThemeColor('charts.green'));
    case 'failed':
      return new vscode.ThemeIcon('error', new vscode.ThemeColor('errorForeground'));
    case 'cancelled':
      return cancelCause?.kind === 'user'
        ? new vscode.ThemeIcon('stop-circle')
        : new vscode.ThemeIcon('circle-slash', new vscode.ThemeColor('charts.orange'));
    case 'corrupt':
      return new vscode.ThemeIcon('warning', new vscode.ThemeColor('charts.orange'));
    default:
      return new vscode.ThemeIcon('circle-outline');
  }
}

