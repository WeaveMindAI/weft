import type {
  CatalogEntry,
  LiveDataItem,
  NodeDefinition,
  PortDefinition,
} from '../../shared/protocol';
import type { NodeExecution } from './exec-types';

// Shared data bag every node component receives as `data` prop.
// Mirrors v1's `data: { ... }` on xyflow nodes so the rendering code
// can lift straight out of the parity specs.
export interface NodeViewData {
  node: NodeDefinition;
  catalog: CatalogEntry | null;
  wiredInputs: Set<string>;
  executions: NodeExecution[];
  liveData?: LiveDataItem[];
  onConfigChange: (nodeId: string, key: string, value: unknown) => void;
  onLabelChange: (nodeId: string, label: string | null) => void;
  onPortsChange: (
    nodeId: string,
    changes: { inputs?: PortDefinition[]; outputs?: PortDefinition[] },
  ) => void;
}
