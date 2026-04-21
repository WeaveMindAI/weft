import type {
  CatalogEntry,
  NodeDefinition,
  PortDefinition,
} from '../../shared/protocol';
import type { NodeExecStatus } from './exec-types';

export interface NodeViewData {
  node: NodeDefinition;
  catalog: CatalogEntry | null;
  wiredInputs: Set<string>;
  exec: {
    status: NodeExecStatus;
    input?: unknown;
    output?: unknown;
    error?: string;
  };
  liveData?: Array<{ label: string; type: 'text' | 'image' | 'progress'; data: unknown }>;
  onConfigChange: (nodeId: string, key: string, value: unknown) => void;
  onLabelChange: (nodeId: string, label: string | null) => void;
  onPortsChange: (
    nodeId: string,
    changes: { inputs?: PortDefinition[]; outputs?: PortDefinition[] },
  ) => void;
}
