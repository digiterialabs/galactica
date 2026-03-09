"use client";

import { useEffect, useState } from "react";
import type { ClusterState } from "@/lib/types";

export function useClusterState(pollInterval = 5000) {
  const [state, setState] = useState<ClusterState>({
    nodes: [],
    models: [],
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;

    async function fetchState() {
      try {
        // TODO: fetch from gateway API
        if (mounted) {
          setLoading(false);
        }
      } catch (err) {
        if (mounted) {
          setError(err instanceof Error ? err.message : "Unknown error");
          setLoading(false);
        }
      }
    }

    fetchState();
    const interval = setInterval(fetchState, pollInterval);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [pollInterval]);

  return { state, loading, error };
}
