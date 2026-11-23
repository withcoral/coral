import { expect, inject } from 'vitest'

import type { ProxiedRequest } from './round-robin-proxy.ts'

export type { ProxiedRequest }

/**
 * The two Coral Instances behind the multi-instance suite's round-robin proxy.
 * Reef and the MCP client reach Coral only through the proxy, which alternates
 * requests between instances unless a scenario sends them to one.
 */
export function coralInstances() {
  const { instances } = inject('reel').target
  if (!instances) throw new Error('This scenario requires the multi-instance suite')
  const control = async <T>(path: string, body?: object): Promise<T> => {
    const response = await fetch(`${instances.controlURL}${path}`, {
      method: body ? 'POST' : 'GET',
      headers: body ? { 'Content-Type': 'application/json' } : {},
      body: body && JSON.stringify(body),
    })
    const answer = await response.json()
    if (!response.ok) throw new Error(`Proxy control ${path}: ${answer.error ?? response.status}`)
    return answer as T
  }
  const requests = () => control<ProxiedRequest[]>('/requests')
  /** Sends every request through the proxy to `instance` while `act` runs. */
  const on = async <T>(instance: number, act: () => Promise<T>): Promise<T> => {
    await control('/pin', { instance })
    try {
      return await act()
    } finally {
      await control('/pin', { instance: null })
    }
  }
  return {
    /** The other instance of the pair. */
    other: (instance: number) => 1 - instance,
    on,
    /**
     * Starts a Run through `instance` and returns its name. Every MCP request
     * must have reached `instance`; the proxy follows the pin since no MCP
     * request carries a session. Either instance may then hold the Run: the
     * one whose Run Executor took it. Read it with `holder`.
     */
    async startOn(instance: number, start: () => Promise<string>) {
      const before = (await requests()).length
      const run = await on(instance, start)
      const mcp = (await requests()).slice(before).filter(({ front }) => front === 'mcp')
      expect(mcp.length).toBeGreaterThan(0)
      expect(mcp.filter((request) => request.instance !== instance)).toEqual([])
      return run
    },
    /**
     * The instance that holds the Run's stream, read from the Run row's holder
     * once the Run has its start mark. The instances start no local Run
     * Executor, so a dialed one runs every Run.
     */
    async holder(run: string) {
      const read = () =>
        control<{
          holder: string | null
          started: boolean
          kind: string | null
          instance: number | null
        }>(`/holder?run=${encodeURIComponent(run)}`)
      await expect.poll(async () => (await read()).started, { timeout: 15_000 }).toBe(true)
      const { holder, kind, instance } = await read()
      expect(kind).toBe('dialed')
      if (instance === null) throw new Error(`${run} is held by ${holder ?? 'no Coral Instance'}`)
      return instance
    },
    requests,
    /** The requests forwarded while `act` runs. */
    async during(act: () => Promise<void>) {
      const before = (await requests()).length
      await act()
      return (await requests()).slice(before)
    },
  }
}

/**
 * Asserts that no request reached its client as unavailable: no HTTP 503 and
 * no gRPC UNAVAILABLE. Coral answers UNAVAILABLE for a request not yet
 * recorded, as it once did for a Run owned by another Coral Instance.
 */
export function expectNoUnavailable(requests: ProxiedRequest[]) {
  expect(
    requests.filter(({ status, grpcStatus }) => status === 503 || grpcStatus === '14'),
  ).toEqual([])
}
