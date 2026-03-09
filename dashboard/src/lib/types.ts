export type ChatMessage = {
  role: string;
  content: string;
};

export type GatewayOverview = {
  node_count: number;
  loaded_model_count: number;
  pool_breakdown: Record<string, number>;
};

export type GatewayNode = {
  node_id: string;
  hostname: string;
  status: string;
  last_heartbeat?: string;
  version: string;
  pool: string;
  runtime_backends: string[];
  accelerators: {
    type: number;
    name: string;
    compute_capability: string;
    vram_bytes: number;
  }[];
  locality: Record<string, string>;
  system_memory?: {
    total_bytes: number;
    available_bytes: number;
  };
};

export type GatewayLoadedModel = {
  instance_id: string;
  model_id: string;
  node_id: string;
  runtime: string;
};

export type GatewayClusterResponse = {
  overview: GatewayOverview;
  nodes: GatewayNode[];
  loaded_models: GatewayLoadedModel[];
};

export type GatewayModel = {
  model_id: string;
  name: string;
  family: string;
  chat_template: string;
  metadata: Record<string, string>;
  variants: {
    runtime: string;
    quantization: string;
    format: string;
    size_bytes: number;
    compatible_accelerators: number[];
  }[];
};

export type GatewayModelsResponse = {
  models: GatewayModel[];
};

export type GatewayEvent = {
  sequence: number;
  event_id: string;
  timestamp?: string;
  payload: {
    type: string;
    [key: string]: unknown;
  };
};

export type GatewayEventsResponse = {
  last_sequence: number;
  events: GatewayEvent[];
};

export type GatewayAuditRecord = {
  event_id: string;
  timestamp?: string;
  actor: string;
  action: string;
  resource: string;
  outcome: string;
  details: Record<string, string>;
};

export type GatewayAuditResponse = {
  records: GatewayAuditRecord[];
};

export type GatewaySessionSummary = {
  session_id: string;
  tenant_id: string;
  model: string;
  updated_at: string;
  turn_count: number;
};

export type GatewaySessionsResponse = {
  sessions: GatewaySessionSummary[];
};

export type GatewaySession = {
  session_id: string;
  tenant_id: string;
  model: string;
  created_at: string;
  updated_at: string;
  turns: {
    role: string;
    content: string;
    timestamp: string;
  }[];
};

export type GatewaySettingsResponse = {
  deployment_mode: string;
  auth: {
    tenant_id: string;
    require_mtls: boolean;
    max_requests_per_minute: number;
    allowed_models: string[];
    allowed_node_pools: string[];
  };
  gateway: {
    global_requests_per_minute: number;
    default_tenant_requests_per_minute: number;
    session_history_limit: number;
  };
};

export type GatewaySnapshot = {
  cluster: GatewayClusterResponse;
  models: GatewayModelsResponse;
  events: GatewayEventsResponse;
  audit: GatewayAuditResponse;
  sessions: GatewaySessionsResponse;
  settings: GatewaySettingsResponse;
};
