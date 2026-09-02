// Classify one node-data update: which source ops its config payload
// emits, and whether it is a PURE collapse/expand toggle.
//
// A toggle is `expanded` actually CHANGING value (not merely present:
// resize senders spread the whole config, which always carries the
// current `expanded`) AND nothing else riding along. A bundled gesture
// (the connection pick sends its handle + `expanded: true` together,
// on a node whose config has no `expanded` yet) must NOT classify as a
// toggle: the toggle path runs the heavy collapse machinery
// (visibility rebuild + whole-graph ELK + viewport pin) and writes
// history a second time, where the bundle wants exactly one
// recordEdit. Pure so it is unit-tested per gesture.

import type { EditOp } from '../../../protocol';
import type { NodeDataUpdates } from '../types';
import { diffConfigOps } from './config-diff';

export interface UpdateClass {
	/// Source ops the config payload emits: only keys whose value
	/// actually CHANGED vs the projected config (emitting unchanged
	/// keys would turn a pure layout gesture into phantom source ops
	/// whose round-trip races the layout persist and reverts the
	/// toggle).
	configOps: EditOp[];
	isExpandToggle: boolean;
}

export function classifyUpdate(
	nodeId: string,
	updates: NodeDataUpdates,
	liveConfig: Record<string, unknown> | undefined,
	isLoopConfig: boolean,
): UpdateClass {
	const priorExpanded = (liveConfig as Record<string, boolean> | undefined)?.expanded;
	const nextExpanded = (updates.config as Record<string, boolean> | undefined)?.expanded;
	const configOps = ('config' in updates)
		? diffConfigOps(nodeId, updates.config as Record<string, unknown>, liveConfig ?? {}, isLoopConfig)
		: [];
	const carriesOtherWork = configOps.length > 0
		|| ('label' in updates) || ('inputs' in updates) || ('outputs' in updates)
		|| ('portLiterals' in updates) || ('portValueForm' in updates);
	return {
		configOps,
		isExpandToggle: nextExpanded !== undefined && nextExpanded !== priorExpanded && !carriesOtherWork,
	};
}
