import { getContext, setContext } from 'svelte';

/// Registry of every mounted node's field-editor `flush`. ProjectEditorInner
/// provides it; each ProjectNode registers its editor on mount and unregisters
/// on destroy. `flushAllPendingSaves` flushes them all so the last <700ms of
/// typing isn't lost on teardown paths (window hidden, panel close) that don't
/// fire a field `blur`.
const KEY = Symbol('weft.fieldEditorRegistry');

export interface FieldEditorRegistry {
  register(flush: () => void): () => void; // returns an unregister fn
  flushAll(): void;
}

export function provideFieldEditorRegistry(): FieldEditorRegistry {
  const flushers = new Set<() => void>();
  const registry: FieldEditorRegistry = {
    register(flush) {
      flushers.add(flush);
      return () => flushers.delete(flush);
    },
    flushAll() {
      for (const flush of flushers) flush();
    },
  };
  setContext(KEY, registry);
  return registry;
}

/// Consumed by a node to register its field-editor flush. Returns undefined if
/// rendered outside a provider (defensive; the node just won't be flushed).
export function useFieldEditorRegistry(): FieldEditorRegistry | undefined {
  return getContext<FieldEditorRegistry | undefined>(KEY);
}
