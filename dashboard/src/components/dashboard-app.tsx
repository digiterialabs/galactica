"use client";

import { useEffect, useMemo, useState } from "react";

import { useClusterState } from "@/hooks/use-cluster-state";
import { sendChatCompletion } from "@/lib/api";
import { ChatMessage, GatewaySession } from "@/lib/types";

type DashboardChatMessage = ChatMessage & {
  pending?: boolean;
  timestamp?: string;
};

function bytesLabel(bytes?: number) {
  if (!bytes) {
    return "n/a";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unit]}`;
}

function timeLabel(timestamp?: string) {
  if (!timestamp) {
    return "now";
  }
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return "now";
  }
  return date.toLocaleTimeString([], {
    hour: "numeric",
    minute: "2-digit",
  });
}

function roleLabel(role: string) {
  switch (role) {
    case "assistant":
      return "Assistant";
    case "system":
      return "System";
    default:
      return "You";
  }
}

function sessionToChatMessages(session: GatewaySession): DashboardChatMessage[] {
  return session.turns.map((turn) => ({
    role: turn.role,
    content: turn.content,
    timestamp: turn.timestamp,
  }));
}

export function DashboardApp() {
  const {
    snapshot,
    selectedSession,
    sessionMap,
    loading,
    error,
    selectSession,
    refreshSession,
  } = useClusterState();
  const [draft, setDraft] = useState("Summarize the current cluster health.");
  const [chatMessages, setChatMessages] = useState<DashboardChatMessage[]>([]);
  const [chatError, setChatError] = useState<string | null>(null);
  const [isSending, setIsSending] = useState(false);
  const [sessionId, setSessionId] = useState<string | undefined>(undefined);
  const [stream, setStream] = useState(true);
  const [selectedModel, setSelectedModel] = useState<string>("");
  const [temperature, setTemperature] = useState(0.7);
  const [maxTokens, setMaxTokens] = useState(256);

  useEffect(() => {
    if (!selectedSession) {
      return;
    }
    setChatMessages(sessionToChatMessages(selectedSession));
    setSessionId(selectedSession.session_id);
    setSelectedModel(selectedSession.model);
    setChatError(null);
  }, [selectedSession]);

  const sectionLinks = [
    ["Overview", "#overview"],
    ["Nodes", "#nodes"],
    ["Models", "#models"],
    ["Chat", "#playground"],
    ["Events", "#events"],
    ["Settings", "#settings"],
  ] as const;

  const availableModels = snapshot?.models.models ?? [];
  const overview = snapshot?.cluster.overview;
  const nodes = snapshot?.cluster.nodes ?? [];
  const loadedModels = snapshot?.cluster.loaded_models ?? [];
  const sessions = snapshot?.sessions.sessions ?? [];
  const events = snapshot?.events.events ?? [];
  const audit = snapshot?.audit.records ?? [];

  const activeModel = useMemo(() => {
    if (selectedModel) {
      return selectedModel;
    }
    return availableModels[0]?.model_id ?? "";
  }, [availableModels, selectedModel]);

  const activeSessionSummary = sessionId ? sessionMap.get(sessionId) : undefined;
  const turnCount = chatMessages.filter((message) => message.role === "user").length;

  function startNewChat() {
    setSessionId(undefined);
    setChatMessages([]);
    setChatError(null);
    setDraft("");
  }

  function handleModelChange(modelId: string) {
    if (modelId === activeModel) {
      return;
    }
    setSelectedModel(modelId);
    if (chatMessages.length > 0 || sessionId) {
      setSessionId(undefined);
      setChatMessages([]);
    }
    setChatError(null);
  }

  async function openSession(sessionKey: string) {
    setChatError(null);
    await selectSession(sessionKey);
  }

  async function sendMessage() {
    const content = draft.trim();
    if (!activeModel || !content || isSending) {
      return;
    }

    const timestamp = new Date().toISOString();
    const userMessage: DashboardChatMessage = {
      role: "user",
      content,
      timestamp,
    };
    const assistantMessage: DashboardChatMessage = {
      role: "assistant",
      content: "",
      pending: true,
      timestamp,
    };
    const nextConversation = [
      ...chatMessages.map(({ role, content: messageContent }) => ({
        role,
        content: messageContent,
      })),
      { role: "user", content },
    ];

    setIsSending(true);
    setChatError(null);
    setDraft("");
    setChatMessages([...chatMessages, userMessage, assistantMessage]);

    try {
      const result = await sendChatCompletion({
        model: activeModel,
        messages: nextConversation,
        sessionId,
        stream,
        temperature,
        maxTokens,
        onChunk: (delta) => {
          setChatMessages((current) => {
            const next = [...current];
            const lastMessage = next.at(-1);
            if (!lastMessage || lastMessage.role !== "assistant") {
              return current;
            }
            next[next.length - 1] = {
              ...lastMessage,
              content: `${lastMessage.content}${delta}`,
              pending: false,
            };
            return next;
          });
        },
      });

      setChatMessages((current) => {
        const next = [...current];
        const lastMessage = next.at(-1);
        if (!lastMessage || lastMessage.role !== "assistant") {
          next.push({
            role: "assistant",
            content: result.content,
            timestamp: new Date().toISOString(),
          });
          return next;
        }
        next[next.length - 1] = {
          ...lastMessage,
          content: result.content || lastMessage.content,
          pending: false,
          timestamp: new Date().toISOString(),
        };
        return next;
      });

      const nextSessionId = result.sessionId ?? sessionId;
      setSessionId(nextSessionId);
      if (nextSessionId) {
        void refreshSession(nextSessionId);
      }
    } catch (submitError) {
      setChatError(
        submitError instanceof Error ? submitError.message : "Unknown inference error",
      );
      setChatMessages((current) => {
        const lastMessage = current.at(-1);
        if (lastMessage?.role === "assistant" && !lastMessage.content.trim()) {
          return current.slice(0, -1);
        }
        return current;
      });
    } finally {
      setIsSending(false);
    }
  }

  return (
    <div className="dashboard-shell">
      <aside className="dashboard-sidebar">
        <div className="brand-lockup">
          <p className="eyebrow">Galactica</p>
          <h1>Phase 3 Operations Deck</h1>
          <p className="subtle">
            Durable state, auth, observability, and live cluster orchestration from one screen.
          </p>
        </div>

        <nav className="side-nav">
          {sectionLinks.map(([label, href]) => (
            <a key={href} href={href}>
              {label}
            </a>
          ))}
        </nav>

        <div className="status-card">
          <span className="status-dot" />
          <div>
            <strong>{loading ? "Syncing" : "Live polling"}</strong>
            <p>{error ? "Gateway fetch issue detected" : "Polling the admin API every 5s"}</p>
          </div>
        </div>
      </aside>

      <main className="dashboard-main">
        {error ? <div className="error-banner">{error}</div> : null}

        <section id="overview" className="section-grid hero-grid">
          <article className="hero-card">
            <p className="eyebrow">Cluster overview</p>
            <div className="metric-row">
              <div>
                <span className="metric-value">{overview?.node_count ?? 0}</span>
                <span className="metric-label">nodes</span>
              </div>
              <div>
                <span className="metric-value">{overview?.loaded_model_count ?? 0}</span>
                <span className="metric-label">loaded models</span>
              </div>
              <div>
                <span className="metric-value">{sessions.length}</span>
                <span className="metric-label">tracked sessions</span>
              </div>
            </div>
            <div className="pool-breakdown">
              {Object.entries(overview?.pool_breakdown ?? {}).map(([pool, count]) => (
                <div key={pool} className="pool-pill">
                  <strong>{count}</strong>
                  <span>{pool}</span>
                </div>
              ))}
            </div>
          </article>

          <article className="panel">
            <p className="eyebrow">Recent audit</p>
            <ul className="compact-list">
              {audit.slice(0, 4).map((record) => (
                <li key={record.event_id}>
                  <strong>{record.action}</strong>
                  <span>
                    {record.actor}
                    {" -> "}
                    {record.resource}
                  </span>
                </li>
              ))}
            </ul>
          </article>
        </section>

        <section id="nodes" className="section-grid two-column">
          <article className="panel">
            <div className="section-header">
              <h2>Nodes</h2>
              <p>Pool-aware view of cluster capacity and locality.</p>
            </div>
            <div className="data-table">
              {nodes.map((node) => (
                <div key={node.node_id} className="table-row">
                  <div>
                    <strong>{node.hostname}</strong>
                    <span>{node.node_id}</span>
                  </div>
                  <div>
                    <strong>{node.status}</strong>
                    <span>{node.pool}</span>
                  </div>
                  <div>
                    <strong>{node.runtime_backends.join(", ") || "n/a"}</strong>
                    <span>{bytesLabel(node.system_memory?.total_bytes)}</span>
                  </div>
                </div>
              ))}
            </div>
          </article>

          <article className="panel">
            <div className="section-header">
              <h2>Loaded models</h2>
              <p>What is currently resident across the cluster.</p>
            </div>
            <ul className="compact-list">
              {loadedModels.map((model) => (
                <li key={model.instance_id}>
                  <strong>{model.model_id}</strong>
                  <span>
                    {model.runtime} on {model.node_id}
                  </span>
                </li>
              ))}
            </ul>
          </article>
        </section>

        <section id="models" className="section-grid">
          <article className="panel">
            <div className="section-header">
              <h2>Model registry</h2>
              <p>Available manifests and runtime variants surfaced through the gateway.</p>
            </div>
            <div className="model-grid">
              {availableModels.map((model) => (
                <button
                  key={model.model_id}
                  className={`model-card ${activeModel === model.model_id ? "selected" : ""}`}
                  onClick={() => handleModelChange(model.model_id)}
                  type="button"
                >
                  <strong>{model.name}</strong>
                  <span>{model.family}</span>
                  <div className="variant-row">
                    {model.variants.map((variant) => (
                      <code key={`${model.model_id}-${variant.runtime}-${variant.quantization}`}>
                        {variant.runtime}/{variant.quantization}
                      </code>
                    ))}
                  </div>
                </button>
              ))}
            </div>
          </article>
        </section>

        <section id="playground" className="section-grid two-column">
          <article className="panel chat-panel">
            <div className="section-header">
              <div>
                <h2>Chat console</h2>
                <p>Multi-turn tenant chat routed through the live gateway session API.</p>
              </div>
              <button className="secondary-button" onClick={startNewChat} type="button">
                New chat
              </button>
            </div>

            <div className="chat-toolbar">
              <div className="chat-chip">
                <span className="setting-label">Model</span>
                <strong>{activeModel || "No model available"}</strong>
              </div>
              <div className="chat-chip">
                <span className="setting-label">Session</span>
                <strong>{sessionId ?? "ephemeral"}</strong>
              </div>
              <div className="chat-chip">
                <span className="setting-label">Turns</span>
                <strong>{turnCount}</strong>
              </div>
            </div>

            <div className="chat-transcript">
              {chatMessages.length === 0 ? (
                <div className="chat-empty">
                  <strong>No conversation yet.</strong>
                  <p>
                    Start a new thread here or pick a session from the event rail to continue it.
                  </p>
                </div>
              ) : (
                chatMessages.map((message, index) => (
                  <article
                    key={`${message.timestamp ?? "now"}-${message.role}-${index}`}
                    className={`chat-message ${message.role}`}
                  >
                    <div className="chat-meta">
                      <strong>{roleLabel(message.role)}</strong>
                      <span>{message.pending ? "Thinking..." : timeLabel(message.timestamp)}</span>
                    </div>
                    <div className={`chat-bubble ${message.pending ? "pending" : ""}`}>
                      {message.content || "Waiting for first tokens..."}
                    </div>
                  </article>
                ))
              )}
            </div>

            <div className="composer">
              <label className="field">
                <span>Message</span>
                <textarea
                  rows={5}
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                  placeholder="Ask Galactica about cluster health, loaded models, or deployment status."
                />
              </label>

              <div className="composer-actions">
                <p className="subtle">
                  {stream
                    ? "Streaming is enabled. Partial assistant output will land in the thread."
                    : "Buffered mode is enabled. The assistant reply will land when complete."}
                </p>
                <button
                  className="primary-button"
                  disabled={isSending || !activeModel || !draft.trim()}
                  onClick={() => void sendMessage()}
                  type="button"
                >
                  {isSending ? "Sending..." : "Send message"}
                </button>
              </div>

              {chatError ? <div className="error-banner">{chatError}</div> : null}
            </div>
          </article>

          <article className="panel">
            <div className="section-header">
              <h2>Chat controls</h2>
              <p>Change routing and response behavior for the active thread.</p>
            </div>

            <label className="field">
              <span>Model</span>
              <select value={activeModel} onChange={(event) => handleModelChange(event.target.value)}>
                {availableModels.map((model) => (
                  <option key={model.model_id} value={model.model_id}>
                    {model.name}
                  </option>
                ))}
              </select>
            </label>

            <div className="parameter-grid">
              <label className="field">
                <span>Temperature</span>
                <input
                  min={0}
                  max={2}
                  step={0.1}
                  type="number"
                  value={temperature}
                  onChange={(event) => {
                    const next = Number.parseFloat(event.target.value);
                    if (!Number.isNaN(next)) {
                      setTemperature(next);
                    }
                  }}
                />
              </label>

              <label className="field">
                <span>Max tokens</span>
                <input
                  min={32}
                  max={4096}
                  step={32}
                  type="number"
                  value={maxTokens}
                  onChange={(event) => {
                    const next = Number.parseInt(event.target.value, 10);
                    if (!Number.isNaN(next)) {
                      setMaxTokens(next);
                    }
                  }}
                />
              </label>
            </div>

            <label className="toggle">
              <input
                checked={stream}
                onChange={(event) => setStream(event.target.checked)}
                type="checkbox"
              />
              <span>Stream assistant deltas into the thread</span>
            </label>

            <div className="chat-context">
              <div>
                <span className="setting-label">Active session</span>
                <strong>{sessionId ?? "Not persisted yet"}</strong>
              </div>
              <div>
                <span className="setting-label">Gateway summary</span>
                <strong>
                  {activeSessionSummary
                    ? `${activeSessionSummary.turn_count} turns · ${activeSessionSummary.updated_at}`
                    : "Waiting for first response"}
                </strong>
              </div>
              <div>
                <span className="setting-label">Selected admin thread</span>
                <strong>{selectedSession?.session_id ?? "None"}</strong>
              </div>
            </div>

            <p className="subtle">
              Switching models starts a fresh conversation so the gateway session stays
              single-model.
            </p>
          </article>
        </section>

        <section id="events" className="section-grid two-column">
          <article className="panel">
            <div className="section-header">
              <h2>Sessions</h2>
              <p>Recent session history tracked by the gateway middleware.</p>
            </div>
            <div className="session-list">
              {sessions.map((session) => (
                <button
                  key={session.session_id}
                  className={`session-card ${session.session_id === sessionId ? "active" : ""}`}
                  onClick={() => void openSession(session.session_id)}
                  type="button"
                >
                  <strong>{session.session_id}</strong>
                  <span>
                    {session.model} · {session.turn_count} turns
                  </span>
                </button>
              ))}
            </div>
            {selectedSession ? (
              <div className="session-detail">
                <h3>{selectedSession.session_id}</h3>
                {selectedSession.turns.map((turn, index) => (
                  <div key={`${turn.timestamp}-${index}`} className="turn">
                    <strong>{turn.role}</strong>
                    <p>{turn.content}</p>
                  </div>
                ))}
              </div>
            ) : null}
          </article>

          <article className="panel">
            <div className="section-header">
              <h2>Event feed</h2>
              <p>Sequenced cluster history exposed via the admin API.</p>
            </div>
            <ul className="compact-list">
              {events.slice(0, 8).map((event) => (
                <li key={event.event_id}>
                  <strong>{event.payload.type}</strong>
                  <span>
                    seq {event.sequence}
                    {event.timestamp ? ` · ${event.timestamp}` : ""}
                  </span>
                </li>
              ))}
            </ul>
          </article>
        </section>

        <section id="settings" className="section-grid">
          <article className="panel">
            <div className="section-header">
              <h2>Settings</h2>
              <p>Deployment and tenant policy values currently surfaced by the gateway.</p>
            </div>
            <div className="settings-grid">
              <div>
                <span className="setting-label">Deployment mode</span>
                <strong>{snapshot?.settings.deployment_mode ?? "n/a"}</strong>
              </div>
              <div>
                <span className="setting-label">Tenant limit</span>
                <strong>
                  {snapshot?.settings.auth.max_requests_per_minute ?? 0} req/min
                </strong>
              </div>
              <div>
                <span className="setting-label">Global limit</span>
                <strong>
                  {snapshot?.settings.gateway.global_requests_per_minute ?? 0} req/min
                </strong>
              </div>
              <div>
                <span className="setting-label">mTLS required</span>
                <strong>{snapshot?.settings.auth.require_mtls ? "Yes" : "No"}</strong>
              </div>
            </div>
          </article>
        </section>
      </main>
    </div>
  );
}
