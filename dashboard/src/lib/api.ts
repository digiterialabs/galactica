import {
  ChatMessage,
  GatewayAuditResponse,
  GatewayClusterResponse,
  GatewayEventsResponse,
  GatewayModelsResponse,
  GatewaySession,
  GatewaySessionsResponse,
  GatewaySettingsResponse,
  GatewaySnapshot,
} from "@/lib/types";

const gatewayUrl =
  process.env.NEXT_PUBLIC_GALACTICA_GATEWAY_URL ?? "http://127.0.0.1:8080";
const apiKey = process.env.NEXT_PUBLIC_GALACTICA_API_KEY ?? "galactica-dev-key";

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(`${gatewayUrl}${path}`, {
    headers: {
      "x-api-key": apiKey,
    },
    cache: "no-store",
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${body}`);
  }

  return (await response.json()) as T;
}

export async function fetchSnapshot(
  sinceSequence: number,
): Promise<GatewaySnapshot> {
  const [cluster, models, events, audit, sessions, settings] = await Promise.all([
    fetchJson<GatewayClusterResponse>("/admin/cluster"),
    fetchJson<GatewayModelsResponse>("/admin/models"),
    fetchJson<GatewayEventsResponse>(`/admin/events?since_sequence=${sinceSequence}`),
    fetchJson<GatewayAuditResponse>("/admin/audit?limit=20"),
    fetchJson<GatewaySessionsResponse>("/admin/sessions"),
    fetchJson<GatewaySettingsResponse>("/admin/settings"),
  ]);

  return {
    cluster,
    models,
    events,
    audit,
    sessions,
    settings,
  };
}

export async function fetchSession(sessionId: string): Promise<GatewaySession> {
  return fetchJson<GatewaySession>(`/admin/sessions/${sessionId}`);
}

export async function sendChatCompletion(input: {
  model: string;
  messages: ChatMessage[];
  sessionId?: string;
  stream?: boolean;
  temperature?: number;
  maxTokens?: number;
  onChunk?: (delta: string) => void;
}): Promise<{
  content: string;
  sessionId?: string;
}> {
  const response = await fetch(`${gatewayUrl}/v1/chat/completions`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": apiKey,
    },
    body: JSON.stringify({
      model: input.model,
      session_id: input.sessionId,
      stream: Boolean(input.stream),
      temperature: input.temperature,
      max_tokens: input.maxTokens,
      messages: input.messages,
    }),
  });

  if (!response.ok) {
    throw new Error(await response.text());
  }

  const sessionId = response.headers.get("x-session-id") ?? undefined;

  if (!input.stream) {
    const payload = (await response.json()) as { choices?: { message?: { content?: string } }[] };
    return {
      content: payload.choices?.[0]?.message?.content ?? "",
      sessionId,
    };
  }

  const reader = response.body?.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let content = "";

  if (!reader) {
    return { content, sessionId };
  }

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    buffer += decoder.decode(value, { stream: true });
    const frames = buffer.split("\n\n");
    buffer = frames.pop() ?? "";

    for (const frame of frames) {
      const line = frame
        .split("\n")
        .find((candidate) => candidate.startsWith("data: "));
      if (!line) {
        continue;
      }
      const payload = line.slice(6);
      if (payload === "[DONE]") {
        continue;
      }
      const chunk = JSON.parse(payload) as {
        choices?: { delta?: { content?: string } }[];
      };
      const delta = chunk.choices?.[0]?.delta?.content ?? "";
      if (!delta) {
        continue;
      }
      content += delta;
      input.onChunk?.(delta);
    }
  }

  return { content, sessionId };
}
