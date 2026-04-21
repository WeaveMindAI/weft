// Ported from dashboard-v1/src/lib/utils/port-marker.ts. Sole source
// of truth for port visual appearance across ProjectNode and
// GroupNode. Every port renders with one of four states (full,
// empty, half, empty-dotted) and one of three shapes (circle,
// gather-triangle, expand-triangle). Triangles use inline SVG
// background-image URLs because CSS borders cannot stroke slanted
// edges cleanly; the SVG stroke draws the full outline.

import type { PortDefinition } from '../../shared/protocol';

export type LaneMode = 'Gather' | 'Expand' | null | undefined;
export type PortMarkerState = 'full' | 'empty' | 'half' | 'empty-dotted';
export type PortMarkerShape = 'circle' | 'gather' | 'expand';

export function portMarkerShape(laneMode: LaneMode): PortMarkerShape {
  if (laneMode === 'Gather') return 'gather';
  if (laneMode === 'Expand') return 'expand';
  return 'circle';
}

export function inputMarkerState(
  required: boolean,
  inOneOfRequired: boolean,
  isConfigFilled: boolean = false,
): PortMarkerState {
  if (isConfigFilled) return 'empty-dotted';
  if (required) return 'full';
  if (inOneOfRequired) return 'half';
  return 'empty';
}

function trianglePortBackground(
  shape: 'gather' | 'expand',
  state: PortMarkerState,
  color: string,
): string {
  const pts =
    shape === 'gather'
      ? '1,1 11,6 1,11'
      : '11,1 1,6 11,11';

  const halfRect =
    shape === 'gather'
      ? '<rect x="0" y="0" width="6" height="12"/>'
      : '<rect x="6" y="0" width="6" height="12"/>';

  let body: string;
  if (state === 'full') {
    body = `<polygon points="${pts}" fill="${color}" stroke="${color}" stroke-width="1" stroke-linejoin="round"/>`;
  } else if (state === 'empty') {
    body = `<polygon points="${pts}" fill="white" stroke="${color}" stroke-width="1" stroke-linejoin="round"/>`;
  } else if (state === 'empty-dotted') {
    body = `<polygon points="${pts}" fill="white" stroke="${color}" stroke-width="1" stroke-linejoin="round" stroke-dasharray="1.5 1.2"/>`;
  } else {
    body =
      `<defs><clipPath id="h">${halfRect}</clipPath></defs>` +
      `<polygon points="${pts}" fill="white"/>` +
      `<polygon points="${pts}" fill="${color}" clip-path="url(#h)"/>` +
      `<polygon points="${pts}" fill="none" stroke="${color}" stroke-width="1" stroke-linejoin="round"/>`;
  }

  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 12 12">${body}</svg>`;
  return `url("data:image/svg+xml,${encodeURIComponent(svg)}") center/contain no-repeat`;
}

export function portMarkerStyle(
  port: PortDefinition,
  oneOfRequiredPorts: Set<string>,
  configFilledPorts: Set<string>,
  color: string,
  side: 'input' | 'output',
  extraClass: string = '',
): { style: string; class: string } {
  const shape = portMarkerShape(port.laneMode as LaneMode);
  const state: PortMarkerState =
    side === 'input'
      ? inputMarkerState(
          Boolean(port.required),
          oneOfRequiredPorts.has(port.name),
          configFilledPorts.has(port.name),
        )
      : 'full';

  let style: string;
  if (shape === 'circle') {
    if (state === 'full') {
      const borderColor = side === 'output' ? 'white' : color;
      style = `background-color: ${color}; border-color: ${borderColor}`;
    } else if (state === 'half') {
      style = `background: linear-gradient(to right, ${color} 50%, white 50%); border-color: ${color}`;
    } else if (state === 'empty-dotted') {
      style = `background-color: white; border-color: ${color}; border-style: dotted`;
    } else {
      style = `background-color: white; border-color: ${color}`;
    }
  } else {
    style = `background: ${trianglePortBackground(shape, state, color)}; border: none`;
  }

  const baseClass = '!w-3 !h-3';
  const shapeClass = shape === 'circle' ? '!border !rounded-full' : '';
  const cls = [baseClass, shapeClass, extraClass].filter(Boolean).join(' ');
  return { style, class: cls };
}
