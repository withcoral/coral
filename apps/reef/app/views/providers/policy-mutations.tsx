import { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { useFetcher, useFetchers } from 'react-router'

import {
  decodeSelectedTools,
  selectionKey,
  type SelectedTool,
  type SelectionKey,
} from './selection'
import type { ToolOutcome, ToolView, PolicyActionResult } from './types'

type Request = {
  id: string
  action: string
  data: FormData
  onConfirmed?: (confirmed: SelectionKey[]) => void
  targets: SelectedTool[]
}
const targetsOf = (data: FormData): SelectedTool[] => decodeSelectedTools(data) ?? []

type Mutations = {
  /** A bulk submission is in flight, so selection must hold still. An inline
   * row or drawer change locks only its own tool. */
  bulkPending: boolean
  submit: (
    data: FormData,
    action: string,
    /** Set by the bulk form: the confirmed keys leave the selection. */
    onConfirmed?: (confirmed: SelectionKey[]) => void,
  ) => void
  state: (
    provider: string,
    tool: string,
  ) => {
    pending: boolean
    policy?: ToolView['policy']
    /** The last request on this tool did not settle it. Cleared by the next. */
    failure?: ToolFailure
  }
}

export type ToolFailure = { status: 'failed' | 'unconfirmed'; reason: string }
const Context = createContext<Mutations | null>(null)

/** Own requests above filtered rows and the conditional bulk toolbar. Only active
 * requests mount a fetcher, rather than allocating one for every catalog row. */
export function PolicyMutations({ children }: { children: ReactNode }) {
  const [requests, setRequests] = useState<Request[]>([])
  const [failures, setFailures] = useState<ReadonlyMap<SelectionKey, ToolFailure>>(() => new Map())

  const fetchers = useFetchers()
  const complete = useCallback((request: Request, result: PolicyActionResult | undefined) => {
    setRequests((current) => current.filter((item) => item.id !== request.id))

    const settled = settle(request.targets, result)
    // Each row shows its own outcome.
    if (settled.failures.size > 0)
      setFailures((current) => new Map([...current, ...settled.failures]))
    if (settled.confirmed.length > 0) request.onConfirmed?.(settled.confirmed)
  }, [])
  const submit: Mutations['submit'] = (data, action, onConfirmed) => {
    const targets = targetsOf(data)
    // Generated outside the updater, which StrictMode invokes twice.
    const id = crypto.randomUUID()
    // A new attempt supersedes the outcome of the last.
    setFailures((current) => {
      const next = new Map(current)
      for (const target of targets) next.delete(selectionKey(target.provider, target.tool))
      return next.size === current.size ? current : next
    })
    setRequests((current) => {
      // Two events can arrive before React re-renders the disabled controls,
      // so the authoritative overlap check is against the live request list.
      const taken = new Set(
        current.flatMap((item) =>
          item.targets.map((target) => selectionKey(target.provider, target.tool)),
        ),
      )
      if (targets.some((target) => taken.has(selectionKey(target.provider, target.tool))))
        return current
      return [...current, { id, action, data, onConfirmed, targets }]
    })
  }
  const pending = new Map<string, { pending: boolean; policy?: ToolView['policy'] }>()
  const fetchersByKey = new Map(fetchers.map((fetcher) => [fetcher.key, fetcher]))
  for (const request of requests) {
    // A submitted request owns its tools immediately. Waiting for the
    // fetcher to report busy leaves a render where every overlap guard reads
    // "not pending", so a second change could target the same tool.
    const fetcher = fetchersByKey.get(request.id)
    const access = fetcher?.formData?.get('access') ?? request.data.get('access')
    const failed = (fetcher?.data as PolicyActionResult | undefined)?.error
    const policy =
      !failed && (access === 'enabled' || access === 'approval_required' || access === 'disabled')
        ? access
        : undefined
    for (const target of request.targets)
      pending.set(selectionKey(target.provider, target.tool), { pending: true, policy })
  }
  const state: Mutations['state'] = (provider, tool) => {
    const key = selectionKey(provider, tool)
    return pending.get(key) ?? { pending: false, failure: failures.get(key) }
  }
  return (
    <Context.Provider
      value={{
        bulkPending: requests.some((request) => request.onConfirmed !== undefined),
        submit,
        state,
      }}
    >
      {requests.map((request) => (
        <PolicyRequest key={request.id} request={request} complete={complete} />
      ))}
      {children}
    </Context.Provider>
  )
}

function PolicyRequest({
  request,
  complete,
}: {
  request: Request
  complete: (request: Request, result: PolicyActionResult | undefined) => void
}) {
  const fetcher = useFetcher<PolicyActionResult>({ key: request.id })
  const started = useRef(false)
  const observedBusy = useRef(false)
  const completed = useRef(false)
  useEffect(() => {
    if (started.current) return
    started.current = true
    fetcher.submit(request.data, { method: 'post', action: request.action })
  }, [fetcher, request])
  useEffect(() => {
    if (fetcher.state !== 'idle') {
      observedBusy.current = true
      return
    }
    if (!started.current || completed.current) return
    // React can batch submitting → idle away, so a busy render is not guaranteed.
    // A fresh fetcher key carries no earlier result, so any data is this request's
    // outcome and settles it. Without data, only an observed busy state proves the
    // request ran; a still-idle fetcher here has not started yet.
    if (fetcher.data === undefined && !observedBusy.current) return
    completed.current = true
    complete(request, fetcher.data)
  }, [fetcher.state, fetcher.data, request, complete])
  return null
}

const UNCONFIRMED = 'The response was lost. Verify the saved policy before you retry.'

/**
 * Which of a request's targets Coral confirmed, so the page can drop them from
 * the selection and keep the rest for a retry. A rejected request fails every
 * target. A lost response confirms nothing: it is not evidence that nothing
 * changed, so the targets stay selected and the reader is told to verify.
 */
export function settle(targets: SelectedTool[], result: PolicyActionResult | undefined) {
  const lost = result === undefined || (!result.error && !result.message)
  const outcomes: ToolOutcome[] =
    result?.tools ??
    targets.map((target) =>
      lost
        ? { ...target, status: 'unconfirmed' }
        : result.error
          ? { ...target, status: 'failed', reason: result.error }
          : { ...target, status: 'updated' },
    )
  const confirmed: SelectionKey[] = []
  const failures = new Map<SelectionKey, ToolFailure>()
  for (const outcome of outcomes) {
    const key = selectionKey(outcome.provider, outcome.tool)
    if (outcome.status === 'updated') confirmed.push(key)
    else if (outcome.status === 'failed') failures.set(key, outcome)
    else failures.set(key, { status: 'unconfirmed', reason: UNCONFIRMED })
  }
  return { confirmed, failures }
}

export function usePolicyMutations() {
  const mutations = useContext(Context)
  if (!mutations) throw new Error('Policy controls require PolicyMutations')
  return mutations
}
