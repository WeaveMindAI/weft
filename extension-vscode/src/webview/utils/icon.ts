// Resolve a string icon name (from metadata.json's `icon` field) to
// a lucide-svelte component. Unknown names fall back to a generic
// question icon so rendering stays resilient.

import {
  Brain,
  Bug,
  CircleHelp,
  Clock,
  GitBranch,
  Send,
  Type,
  User,
  Webhook,
} from 'lucide-svelte';
import type { Component } from 'svelte';

type IconComponent = Component<any>;

const REGISTRY: Record<string, IconComponent> = {
  Brain: Brain as unknown as IconComponent,
  Bug: Bug as unknown as IconComponent,
  Clock: Clock as unknown as IconComponent,
  GitBranch: GitBranch as unknown as IconComponent,
  Send: Send as unknown as IconComponent,
  Type: Type as unknown as IconComponent,
  User: User as unknown as IconComponent,
  Webhook: Webhook as unknown as IconComponent,
  // Aliases for v1 names.
  BadgeQuestionMark: CircleHelp as unknown as IconComponent,
  HelpCircle: CircleHelp as unknown as IconComponent,
  CircleHelp: CircleHelp as unknown as IconComponent,
};

const FALLBACK = CircleHelp as unknown as IconComponent;

export function resolveIcon(name: string | null | undefined): IconComponent {
  if (!name) return FALLBACK;
  return REGISTRY[name] ?? FALLBACK;
}
