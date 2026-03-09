export interface NodeInfo {
  id: string;
  hostname: string;
  os: string;
  arch: string;
  status: string;
  accelerators: string[];
  memoryTotal: number;
  memoryAvailable: number;
}

export interface ModelInfo {
  id: string;
  name: string;
  family: string;
  availableRuntimes: string[];
  ready: boolean;
}

export interface ClusterState {
  nodes: NodeInfo[];
  models: ModelInfo[];
}
