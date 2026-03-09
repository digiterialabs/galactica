"use client";

import { startTransition, useEffect, useMemo, useState } from "react";

import { fetchSession, fetchSnapshot } from "@/lib/api";
import { GatewaySession, GatewaySnapshot } from "@/lib/types";

const POLL_INTERVAL_MS = 5000;

export function useClusterState() {
  const [snapshot, setSnapshot] = useState<GatewaySnapshot | null>(null);
  const [selectedSession, setSelectedSession] = useState<GatewaySession | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sinceSequence, setSinceSequence] = useState(0);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const nextSnapshot = await fetchSnapshot(sinceSequence);
        if (cancelled) {
          return;
        }
        startTransition(() => {
          setSnapshot(nextSnapshot);
          setSinceSequence(nextSnapshot.events.last_sequence);
          setLoading(false);
          setError(null);
        });
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        setLoading(false);
        setError(
          loadError instanceof Error
            ? loadError.message
            : "Unknown dashboard fetch error",
        );
      }
    }

    void load();
    const interval = window.setInterval(() => {
      void load();
    }, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [sinceSequence]);

  const sessionMap = useMemo(
    () =>
      new Map(
        snapshot?.sessions.sessions.map((session) => [session.session_id, session]) ?? [],
      ),
    [snapshot],
  );

  async function selectSession(sessionId: string) {
    const session = await fetchSession(sessionId);
    setSelectedSession(session);
  }

  return {
    snapshot,
    selectedSession,
    sessionMap,
    loading,
    error,
    selectSession,
    refreshSession: selectSession,
  };
}
