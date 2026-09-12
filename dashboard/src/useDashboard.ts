import { useCallback, useEffect, useState } from "react";
import { fetchDashboard } from "./api";
import type { DashboardData } from "./types";

export function useDashboard() {
  const [revision, setRevision] = useState(0);
  const [data, setData] = useState<DashboardData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchDashboard(controller.signal)
      .then((next) => { setData(next); setUpdatedAt(new Date()); })
      .catch((reason: unknown) => {
        if (!(reason instanceof DOMException && reason.name === "AbortError")) {
          setError(reason instanceof Error ? reason.message : "Unable to load telemetry.");
        }
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [revision]);

  const refresh = useCallback(() => setRevision((current) => current + 1), []);
  return { data, error, loading, updatedAt, refresh };
}

