import { Meter } from '@/wax/components/charts'
import { Icon } from '@/wax/components/icon'
import * as Table from '@/wax/components/table'
import { Typography } from '@/wax/components/typography'
import type { ContextFlow } from './context-flow'
import { formatBytes } from './context-impact'
import { offset } from './duration'
import { RecordLabel } from './record'
import * as styles from './run-lenses.css'
import type { TimelineSpan } from './timeline'
import type { SavedFunction } from './trace-summary'

/**
 * Where a root's context went, step by step: what each program read from its calls and what it
 * sent the agent, and what each search or describe sent it. The bar compares what each step
 * read or sent with the step that moved the most.
 */
export function ContextBySteps({
  spans,
  flow,
  href,
}: {
  spans: TimelineSpan[]
  flow: ContextFlow
  href: (record: string) => string
}) {
  const steps = spans.flatMap((span) => {
    const own = flow.records.get(span.record.name)
    if (!own || own.into === undefined) return []
    const read = span.record.kind === 'run' ? own.into + (own.keptOut ?? 0) : undefined
    return [{ span, own, read }]
  })
  if (!steps.length)
    return (
      <Typography.BodySmall as="p" variant="tertiary" className={styles.note}>
        Coral measured no context for this run.
      </Typography.BodySmall>
    )
  const most = Math.max(1, ...steps.map(({ own, read }) => read ?? own.into ?? 0))
  return (
    <Table.Container
      ariaLabel="Context by step"
      variant="card"
      layout="fixed"
      density="compact"
      columns={[
        { label: 'Step', width: { fill: 3 } },
        { label: 'Read by its program', width: { fill: 2 } },
        { label: 'Sent to agent', width: 140, align: 'right' },
        { label: 'Kept out', width: 120, align: 'right' },
      ]}
    >
      <Table.Head />
      <Table.Body>
        {steps.map(({ span, own, read }) => (
          <Table.Row key={span.record.name} to={href(span.record.name)} preventScrollReset>
            <Table.Cell>
              <Table.RowLink to={href(span.record.name)}>
                <RecordLabel row={span.record} />
              </Table.RowLink>
            </Table.Cell>
            <Table.Cell>
              {read === undefined ? (
                <Typography.BodySmall variant="tertiary">—</Typography.BodySmall>
              ) : (
                <span className={styles.meterCell}>
                  <Typography.BodySmall variant="tertiary" className={styles.figure}>
                    {formatBytes(read)}
                  </Typography.BodySmall>
                  <Meter value={read} max={most} size="small" className={styles.meter} />
                </span>
              )}
            </Table.Cell>
            <Table.Cell mono>{formatBytes(own.into ?? 0)}</Table.Cell>
            <Table.Cell mono>
              {own.keptOut ? (
                <Typography.BodySmall variant="success">
                  {formatBytes(own.keptOut)}
                </Typography.BodySmall>
              ) : (
                '—'
              )}
            </Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Container>
  )
}

/**
 * The functions a root saved: each by its name, linked to Functions when a later Run called it
 * by its path, then when it was first saved, how often it was revised, and whether a Run that
 * succeeded called it after its last save.
 */
export function SavedFunctionsTable({
  functions,
  workspace,
  origin,
  href,
}: {
  functions: SavedFunction[]
  workspace: string
  origin: number
  href: (record: string) => string
}) {
  return (
    <Table.Container
      ariaLabel="Saved functions"
      variant="card"
      layout="fixed"
      density="compact"
      columns={[
        { label: 'Function', width: { fill: 3 } },
        { label: 'Saved', width: 100 },
        { label: 'Revisions', width: 110 },
        { label: 'Tested', width: 140 },
      ]}
    >
      <Table.Head />
      <Table.Body>
        {functions.map(({ key, path, saves, tested }) => {
          const opens = path
            ? `/${workspace}/functions/${encodeURIComponent(path)}`
            : href(saves[0]!.name)
          return (
            <Table.Row key={key} to={opens}>
              <Table.Cell>
                <Table.RowLink to={opens}>{path ?? <RecordLabel row={saves[0]!} />}</Table.RowLink>
              </Table.Cell>
              <Table.Cell mono>{offset(Date.parse(saves[0]!.startedAt) - origin)}</Table.Cell>
              <Table.Cell mono>{saves.length > 1 ? saves.length - 1 : '—'}</Table.Cell>
              <Table.Cell>
                {tested ? (
                  <span className={styles.meterCell}>
                    <Icon name="CircleCheck" size="14" color="success" />
                    <Typography.BodySmall>Smoke-tested</Typography.BodySmall>
                  </span>
                ) : (
                  <Typography.BodySmall variant="tertiary">Not tested</Typography.BodySmall>
                )}
              </Table.Cell>
            </Table.Row>
          )
        })}
      </Table.Body>
    </Table.Container>
  )
}
