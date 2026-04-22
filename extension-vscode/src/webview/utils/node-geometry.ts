// Ported from dashboard-v1/src/lib/components/project/ProjectEditorInner.svelte:811-830
// (computeMinNodeWidth) and GroupNode.svelte (computeMinHeight).
//
// Both helpers compute sizes in the same coordinate space xyflow
// uses, so nodes size to their port labels instead of clipping.

import type { PortDefinition } from '../../shared/protocol';

const MIN_WIDTH = 200;
const CHAR_WIDTH = 6.5;
const PADDING = 60;
const GAP = 20;

export function computeMinNodeWidth(
  inputs: readonly PortDefinition[],
  outputs: readonly PortDefinition[],
): number {
  const inputNames = inputs.map((p) => p.name + (p.required ? '*' : ''));
  const outputNames = outputs.map((p) => p.name);
  let maxRowWidth = 0;
  const rows = Math.max(inputNames.length, outputNames.length);
  for (let i = 0; i < rows; i++) {
    const leftLen = i < inputNames.length ? inputNames[i].length : 0;
    const rightLen = i < outputNames.length ? outputNames[i].length : 0;
    const rowWidth = (leftLen + rightLen) * CHAR_WIDTH + GAP;
    if (rowWidth > maxRowWidth) maxRowWidth = rowWidth;
  }
  return Math.max(MIN_WIDTH, Math.ceil(maxRowWidth + PADDING));
}

// GroupNode.svelte:168-170 — minimum expanded height from port count.
const GROUP_HEADER_HEIGHT = 36;
const GROUP_PORTS_TOP = 8;
const GROUP_PORT_ROW = 30;
const GROUP_BODY_PADDING = 24;
const GROUP_BODY_MIN = 128;

export function computeGroupMinHeight(
  inputsLen: number,
  outputsLen: number,
): number {
  const portCount = Math.max(inputsLen, outputsLen);
  return (
    GROUP_HEADER_HEIGHT +
    GROUP_PORTS_TOP +
    portCount * GROUP_PORT_ROW +
    GROUP_BODY_PADDING +
    GROUP_BODY_MIN
  );
}

// ProjectNode.svelte:297 — minResizeHeight.
// 2 (accent) + 32 (header) + 16 (content top pad) + 24 (label) + 8
// (label-to-ports gap) + rows * 25 + 80 (body minimum).
export function computeNodeMinResizeHeight(
  inputsLen: number,
  outputsLen: number,
): number {
  return 2 + 32 + 16 + 24 + 8 + Math.max(inputsLen, outputsLen) * 25 + 80;
}
