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
     * Starts a Run through `instance` and returns its name. The instance that
     * accepts a Run holds it, so this also teaches the harness that instance's id.
     * Every MCP request must have reached `instance`; the proxy follows the pin
     * since no MCP request carries a session.
     */
    async startOn(instance: number, start: () => Promise<string>) {
      const before = (await requests()).length
      const run = await on(instance, start)
      const mcp = (await requests()).slice(before).filter(({ front }) => front === 'mcp')
      expect(mcp.length).toBeGreaterThan(0)
      expect(mcp.filter((request) => request.instance !== instance)).toEqual([])
      await control('/accepted', { instance, run })
      return run
    },
    /** The instance whose Run Owner holds the Run, read from the Run row's holder. */
    async holder(run: string) {
      const { holder, instance } = await control<{
        holder: string | null
        instance: number | null
      }>(`/holder?run=${encodeURIComponent(run)}`)
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
