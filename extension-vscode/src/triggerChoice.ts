// The editor's half of the dispatcher's "how do the triggers come down"
// question. Only the dispatcher knows whether a trigger is on (the
// program's or a member's), so the webview sends a verb without a choice;
// when the dispatcher needs one, `weft <verb> --json` fails with an error
// event flagged `needsTriggerChoice`, and the host asks the webview to
// open its picker for that verb instead of showing the refusal.

import type { ActionVerb, CliEvent, TriggerChoiceIntent } from '../../packages/weft-graph/src/protocol';

/** True when `ev` is the CLI's refusal for a missing trigger choice. */
// SYNC: needsTriggerChoice <-> crates/weft-cli/src/progress.rs error_detail
export function isTriggerChoiceRefusal(ev: CliEvent): boolean {
  return ev.phase === 'error' && ev.detail?.needsTriggerChoice === true;
}

/** The picker intent for the verb that was refused. Throws for a verb
 *  whose choice the webview has no picker for: the refusal would
 *  otherwise vanish with nothing shown. */
export function triggerChoiceIntent(verb: ActionVerb): TriggerChoiceIntent {
  switch (verb) {
    case 'resync': return 'resync';
    case 'infra_stop': return 'infraStop';
    case 'infra_terminate': return 'infraTerminate';
    case 'infra_upgrade': return 'infraUpgrade';
    default:
      throw new Error(`triggerChoiceIntent: '${verb}' has no trigger-choice picker`);
  }
}
