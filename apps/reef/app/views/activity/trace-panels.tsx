import { Link } from 'react-router'
import * as DescriptionList from '@/wax/components/description-list'
import { Typography } from '@/wax/components/typography'
import { CallTool, useToolOwner } from './call-tool'
import { contextFlow, type RecordFlow } from './context-flow'
import { formatBytes } from './context-impact'
import { preciseDuration } from './duration'
import { callsByProvider, runCalls, type ProviderCalls } from './trace-summary'
import * as styles from './trace-panels.css'
import type { ActivityTrace } from './types'

/**
 * What a record did to the agent's context, apart: the bytes that reached it, and the tool
 * results a program read and kept from it. A root also states what its programs read in all.
 */
export function ContextRows({ trace, record }: { trace: ActivityTrace; record: string }) {
  const flow = contextFlow(trace)
  const root = record === trace.root.name
  const own: RecordFlow | undefined = root
    ? {
        into: flow.total.into,
        keptOut: Math.max(0, flow.total.providerData - flow.total.output),
      }
    : flow.records.get(record)
  if (!own) return null
  return (
    <>
      {own.into !== undefined && (
        <DescriptionList.Row label="Into context">{formatBytes(own.into)}</DescriptionList.Row>
      )}
      {own.keptOut !== undefined && (
        <DescriptionList.Row label="Kept out">{formatBytes(own.keptOut)}</DescriptionList.Row>
      )}
      {root && flow.total.providerData > 0 && (
        <DescriptionList.Row label="Provider data">
          {`Programs read ${formatBytes(flow.total.providerData)} and returned ${formatBytes(flow.total.output)}`}
        </DescriptionList.Row>
      )}
    </>
  )
}

/** A root's calls by provider: each provider by its logo and name, with its count. */
export function ProviderCallsRow({ trace }: { trace: ActivityTrace }) {
  const providers = callsByProvider(trace)
  if (!providers.length) return null
  return (
    <DescriptionList.Row label="Calls">
      <span className={styles.stack}>
        {providers.map((provider) => (
          <ProviderLine key={provider.callRoot} provider={provider} />
        ))}
      </span>
    </DescriptionList.Row>
  )
}

function ProviderLine({ provider }: { provider: ProviderCalls }) {
  const owner = useToolOwner(provider.callPath)
  const counts = `${provider.count} ${provider.count === 1 ? 'call' : 'calls'}${provider.failed ? ` · ${provider.failed} failed` : ''}`
  return (
    <span className={styles.line}>
      {owner?.provider.displayName ?? provider.callRoot}
      <Typography.BodySmall>{counts}</Typography.BodySmall>
    </span>
  )
}

/**
 * A program's calls: how many, how long they took together and whether they overlapped, then
 * each one, linked to its record.
 */
export function RunCallsRow({
  trace,
  run,
  recordHref,
}: {
  trace: ActivityTrace
  run: string
  recordHref: (name: string) => string
}) {
  const { calls, span, parallel } = runCalls(trace, run)
  if (!calls.length) return null
  return (
    <DescriptionList.Row label="Calls">
      <span className={styles.stack}>
        <span>
          {`${calls.length} ${calls.length === 1 ? 'call' : 'calls'} · ${preciseDuration(span)}`}
          {parallel && ' in parallel'}
        </span>
        {calls.map((call) => (
          <Link key={call.name} to={recordHref(call.name)} className={styles.line}>
            <CallTool callPath={call.callPath} size="small" />
            <Typography.BodySmall>
              {call.endedAt
                ? preciseDuration(Date.parse(call.endedAt) - Date.parse(call.startedAt))
                : 'running'}
            </Typography.BodySmall>
          </Link>
        ))}
      </span>
    </DescriptionList.Row>
  )
}
