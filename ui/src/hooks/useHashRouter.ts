import { useCallback, useSyncExternalStore } from "react";

export type Route = "traces" | "schema-explorer" | "sources";

const VALID_ROUTES: Route[] = ["traces", "schema-explorer", "sources"];

let cachedRoute: Route = "schema-explorer";

function parseHash(): Route {
  const raw = window.location.hash.replace(/^#\/?/, "");
  const route = VALID_ROUTES.find((candidate) => candidate === raw);
  cachedRoute = route ?? "schema-explorer";
  return cachedRoute;
}

parseHash();

const listeners = new Set<() => void>();

function onHashChange() {
  parseHash();
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void) {
  if (listeners.size === 0) {
    window.addEventListener("hashchange", onHashChange);
  }
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) {
      window.removeEventListener("hashchange", onHashChange);
    }
  };
}

function getSnapshot() {
  return cachedRoute;
}

export function useHashRouter() {
  const route = useSyncExternalStore(subscribe, getSnapshot);

  const navigate = useCallback((nextRoute: Route) => {
    const nextHash = `#/${nextRoute}`;
    const sameHash = window.location.hash === nextHash;
    window.location.hash = nextHash;
    if (sameHash) {
      onHashChange();
    }
  }, []);

  return { route, navigate };
}
