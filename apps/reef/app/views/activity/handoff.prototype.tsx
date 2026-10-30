// PROTOTYPE — throwaway. Three variants of the Run Status Page as the one place a
// Human Handoff leads to (withcoral/coral#2739), switchable with `?variant=A|B|C`.
// Decisions stay in memory; nothing posts. Judged in Storybook, then discarded.
import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { useSearchParams } from 'react-router'
import { Banner, Button, Dialog, Typography } from '@/wax/components'
import { AnnotatedCodeBlock, CodeBlock } from '@/wax/components/code-block'
import { CodeChip } from '@/wax/components/code-chip'
import * as DescriptionList from '@/wax/components/description-list'
import { Disclosure } from '@/wax/components/disclosure'
import { Icon } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import * as SettingRow from '@/wax/components/setting-row'
import { Arguments } from '../actions/arguments'
import { PanelHeader } from './panel'
import * as styles from './handoff.prototype.css'

// ---------- data the page would load ----------

export type HandoffRequest =
  | {
      kind: 'approval'
      name: string
      callPath: string
      providerLabel: string
      /** Exact logical arguments, JSON text. */
      arguments: string
      askedAt: string
      /** The Program line that makes the call. */
      line: number
    }
  | {
      kind: 'account_connection'
      name: string
      callPath: string
      providerLabel: string
      askedAt: string
      /** The request's own expiry, which may outlive the Run. */
      expiresAt: string
      line: number
    }

export interface HandoffPage {
  run: {
    /** The four-word Run identifier the assistant repeats. */
    id: string
    intent: string
    /** The AI app the Run came from, as its clientInfo names it. */
    client: string
    workspaceLabel: string
    acceptedAt: string
    /** The one clock: accepted time plus run_timeout_ms. */
    deadlineAt: string
    state: 'running' | 'timed_out' | 'succeeded' | 'failed'
    program: string
  }
  requests: HandoffRequest[]
  /** The request the link opened on (`?request=`), if any. */
  linked: string | null
  /** True when the viewer owns the Workspace and may change a Tool's policy. */
  owner: boolean
}

type Decision = 'approved' | 'declined' | 'connected' | 'released'
type Decisions = Record<string, Decision | undefined>

// ---------- shared bits ----------

const plural = (n: number, unit: string) => `${n} ${unit}${n === 1 ? '' : 's'}`

function useNow() {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [])
  return now
}

function agoLabel(at: string, now: number) {
  const ms = Math.max(0, now - Date.parse(at))
  if (ms < 60_000) return `${plural(Math.max(1, Math.round(ms / 1000)), 'second')} ago`
  return `${plural(Math.round(ms / 60_000), 'minute')} ago`
}

/** No clock on the page (round 2): the assistant said the window, the Run ending is what a late
 * arriver sees. While the Run runs this renders nothing. */
function RunClock({ page }: { page: HandoffPage; long?: boolean }) {
  if (page.run.state === 'running') return null
  return (
    <Pill color="gray" size="small">
      {`run ${page.run.state.replace('_', ' ')}`}
    </Pill>
  )
}

function Asked({ at }: { at: string }) {
  const now = useNow()
  return (
    <Typography.BodySmall variant="tertiary" as="time" dateTime={at}>
      Asked {agoLabel(at, now)}
    </Typography.BodySmall>
  )
}

/** Dropped in round 2: no time line on the connection card. */
function ConnectionWindow(_props: { page: HandoffPage; request: HandoffRequest }) {
  return null
}

/** A gist of the arguments for a one-line row: the first string fields. */
function gist(source: string) {
  try {
    const value = JSON.parse(source) as Record<string, unknown>
    return Object.entries(value)
      .filter(([, v]) => typeof v === 'string' || typeof v === 'number')
      .slice(0, 2)
      .map(([k, v]) => `${k}: ${String(v).slice(0, 40)}`)
      .join(' · ')
  } catch {
    return ''
  }
}

const decisionLabel: Record<Decision, string> = {
  approved: 'Approved',
  declined: 'Denied',
  connected: 'Connected',
  released: 'Continued by policy',
}
const decisionColor: Record<Decision, 'green' | 'red' | 'blue'> = {
  approved: 'green',
  declined: 'red',
  connected: 'green',
  released: 'blue',
}

function DecisionPill({ decision }: { decision: Decision }) {
  return (
    <Pill color={decisionColor[decision]} size="small" role="status">
      {decisionLabel[decision]}
    </Pill>
  )
}

/** Owner's shortcut: change the Tool's policy, confirmed apart from the decision. */
function PolicyShortcut({
  page,
  request,
  onReleased,
  compact,
}: {
  page: HandoffPage
  request: Extract<HandoffRequest, { kind: 'approval' }>
  onReleased: () => void
  compact?: boolean
}) {
  const [open, setOpen] = useState(false)
  if (!page.owner) return null
  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger
        render={<Button.Container variant={compact ? 'bare' : 'linkSubtle'} size="32" />}
      >
        <Button.Icon name="Settings2" />
        <Button.Text>Stop asking for this tool</Button.Text>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="m">
          <Dialog.Title>Stop asking for this tool?</Dialog.Title>
          <Dialog.Description>
            Every run in {page.run.workspaceLabel} then calls {request.callPath} without a question,
            until an owner changes the policy again. The call that waits now continues too. This is
            a workspace change, not a decision on this call alone.
          </Dialog.Description>
          <Dialog.Actions>
            <Button.TextButton variant="secondary" onClick={() => setOpen(false)}>
              Keep asking
            </Button.TextButton>
            <Button.TextButton
              onClick={() => {
                setOpen(false)
                onReleased()
              }}
            >
              Change policy and continue this call
            </Button.TextButton>
          </Dialog.Actions>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function Closing({ page, variant }: { page: HandoffPage; variant: 'card' | 'inline' }) {
  const body = (
    <>
      <Icon name="CircleCheck" size="24" color="inherit" />
      <Typography.BodyLargeStrong>Done.</Typography.BodyLargeStrong>
      <Typography.Body>
        You can close this tab and go back to {page.run.client}. The run continues there.
      </Typography.Body>
    </>
  )
  if (variant === 'inline') return <div className={styles.stackTight}>{body}</div>
  return <div className={styles.closing}>{body}</div>
}

function Ended() {
  return <Banner variant="warning">This run has already ended.</Banner>
}

function useDecisions(page: HandoffPage) {
  const [decisions, setDecisions] = useState<Decisions>({})
  const decide = (name: string, decision: Decision) =>
    setDecisions((d) => ({ ...d, [name]: decision }))
  const open = page.requests.filter((r) => !decisions[r.name])
  const approvals = open.filter((r) => r.kind === 'approval')
  const approveAll = () =>
    setDecisions((d) => {
      const next = { ...d }
      for (const r of approvals) next[r.name] = 'approved'
      return next
    })
  const alive = page.run.state === 'running'
  return {
    decisions,
    decide,
    open,
    approvals,
    approveAll,
    done: alive && open.length === 0,
    alive,
  }
}
type Session = ReturnType<typeof useDecisions>

function RequestButtons({
  request,
  session,
  size,
}: {
  request: HandoffRequest
  session: Session
  size?: '32'
}) {
  const disabled = !session.alive
  if (request.kind === 'account_connection')
    return (
      <>
        <Button.TextButton
          variant="secondary"
          size={size}
          disabled={disabled}
          onClick={() => session.decide(request.name, 'declined')}
        >
          Decline
        </Button.TextButton>
        <Button.TextButton
          size={size}
          disabled={disabled}
          onClick={() => session.decide(request.name, 'connected')}
        >
          Connect {request.providerLabel}
        </Button.TextButton>
      </>
    )
  return (
    <>
      <Button.TextButton
        variant="destructive"
        size={size}
        disabled={disabled}
        onClick={() => session.decide(request.name, 'declined')}
      >
        Deny
      </Button.TextButton>
      <Button.TextButton
        size={size}
        disabled={disabled}
        onClick={() => session.decide(request.name, 'approved')}
      >
        Approve
      </Button.TextButton>
    </>
  )
}

function ApproveAll({ session, variant }: { session: Session; variant?: 'secondary' }) {
  if (session.approvals.length < 2) return null
  return (
    <span className={styles.row}>
      <Button.TextButton variant={variant ?? 'primary'} onClick={session.approveAll}>
        Approve all {session.approvals.length}
      </Button.TextButton>
      <Typography.BodySmall variant="tertiary">
        Each call is recorded on its own.
      </Typography.BodySmall>
    </span>
  )
}

// ---------- Variant A: the handoff sheet ----------
// One column. The Run leads, then every request as a card with its own buttons, then the
// Program. No activity list: the link brought you here for one thing.

export function VariantA({ page }: { page: HandoffPage }) {
  const session = useDecisions(page)
  const title =
    page.requests.length === 1 ? 'needs you once' : `needs you ${page.requests.length} times`
  return (
    <div className={styles.sheet}>
      <header className={styles.sheetHeader}>
        <Typography.BodySmall variant="tertiary">
          From your chat in {page.run.client} · {page.run.workspaceLabel}
        </Typography.BodySmall>
        <Typography.HeadingLarge as="h1" className={styles.runId}>
          {page.run.id}
        </Typography.HeadingLarge>
        <Typography.Body variant="secondary">“{page.run.intent}”</Typography.Body>
        <div className={styles.spread}>
          <Typography.BodyStrong>This run {title}.</Typography.BodyStrong>
          <RunClock page={page} />
        </div>
      </header>

      {!session.alive && <Ended />}
      {session.done && <Closing page={page} variant="card" />}

      <ol className={styles.stack} aria-label="Requests">
        {page.requests.map((request) => {
          const decision = session.decisions[request.name]
          return (
            <li key={request.name} className={styles.requestCard[decision ? 'settled' : 'plain']}>
              <div className={styles.spread}>
                <div className={styles.stackTight}>
                  <div className={styles.row}>
                    <Typography.BodyLargeStrong as="h2">
                      {request.kind === 'approval'
                        ? 'Approve this call?'
                        : `Connect your ${request.providerLabel} account`}
                    </Typography.BodyLargeStrong>
                    {decision && <DecisionPill decision={decision} />}
                  </div>
                  <div className={styles.row}>
                    <CodeChip>{request.callPath}</CodeChip>
                    <Typography.BodySmall variant="tertiary">
                      {request.providerLabel}
                    </Typography.BodySmall>
                    <Asked at={request.askedAt} />
                  </div>
                  <ConnectionWindow page={page} request={request} />
                </div>
              </div>
              {request.kind === 'approval' && <Arguments source={request.arguments} />}
              {request.kind === 'account_connection' && (
                <Typography.BodySmall>
                  The run calls {request.providerLabel} as you. Connect your {request.providerLabel}{' '}
                  account to continue. Other runs that wait on this account continue too.
                </Typography.BodySmall>
              )}
              {!decision && (
                <div className={styles.spread}>
                  {request.kind === 'approval' ? (
                    <PolicyShortcut
                      page={page}
                      request={request}
                      onReleased={() => session.decide(request.name, 'released')}
                    />
                  ) : (
                    <span />
                  )}
                  <div className={styles.cardActions}>
                    <RequestButtons request={request} session={session} />
                  </div>
                </div>
              )}
            </li>
          )
        })}
      </ol>

      <Disclosure title="Program">
        <CodeBlock value={page.run.program} format="typescript" framed />
      </Disclosure>

      {session.alive && !session.done && session.approvals.length > 1 && (
        <div className={styles.stickyBar}>
          <Typography.BodySmall variant="secondary">
            {plural(session.open.length, 'request')} still open
          </Typography.BodySmall>
          <ApproveAll session={session} />
        </div>
      )}
    </div>
  )
}

// ---------- Variant B: the split, as today ----------
// "Needs your action" rows on the left with their quick buttons, the review on the right.
// The review panel gains a Run strip, a step through the Run's requests, and an ending state.

export function VariantB({ page }: { page: HandoffPage }) {
  const session = useDecisions(page)
  const [params, setParams] = useSearchParams()
  const selectedName = params.get('request') ?? page.linked ?? page.requests[0]?.name
  const index = Math.max(
    0,
    page.requests.findIndex((r) => r.name === selectedName),
  )
  const selected = page.requests[index]
  const select = (name: string) =>
    setParams(
      (p) => {
        p.set('request', name)
        return p
      },
      { preventScrollReset: true },
    )
  const next = page.requests[index + 1]
  const prev = page.requests[index - 1]
  const decision = selected ? session.decisions[selected.name] : undefined
  return (
    <div className={styles.split}>
      <div className={styles.stack}>
        <Typography.HeadingSmall as="h2">Needs your action</Typography.HeadingSmall>
        <SettingRow.List>
          {page.requests.map((request) => {
            const d = session.decisions[request.name]
            const question =
              request.kind === 'approval'
                ? `${page.run.intent.replace(/[.!?]+$/, '')}?`
                : `Connect your ${request.providerLabel} account to continue.`
            return (
              <SettingRow.Row
                key={request.name}
                label={question}
                description={
                  <>
                    <CodeChip>{request.callPath}</CodeChip>
                    {request.kind === 'approval' && gist(request.arguments) && (
                      <> · {gist(request.arguments)}</>
                    )}
                    {' · '}
                    <span className={styles.runIdSmall}>{page.run.id}</span>
                    {' · '}
                    <Asked at={request.askedAt} />
                  </>
                }
                action={
                  d ? (
                    <DecisionPill decision={d} />
                  ) : (
                    <>
                      {request.kind === 'approval' && session.alive && (
                        <RequestButtons request={request} session={session} size="32" />
                      )}
                      <Button.TextButton
                        variant="secondary"
                        size="32"
                        onClick={() => select(request.name)}
                        aria-current={request.name === selected?.name ? 'true' : undefined}
                      >
                        {request.kind === 'approval' ? 'Review' : 'Connect account'}
                      </Button.TextButton>
                    </>
                  )
                }
              />
            )
          })}
        </SettingRow.List>
        <div className={styles.historyStub}>
          <Typography.HeadingSmall as="h2">Your recent activity</Typography.HeadingSmall>
          <Typography.BodySmall variant="tertiary">
            (the history table, as today: time, record, state)
          </Typography.BodySmall>
        </div>
      </div>

      <section className={styles.panelFrame} aria-label="Review">
        <div className={styles.runStrip}>
          <div className={styles.spread}>
            <span className={styles.row}>
              <Typography.BodySmall variant="tertiary">Run</Typography.BodySmall>
              <span className={styles.runIdSmall}>{page.run.id}</span>
              <Typography.BodySmall variant="tertiary">from {page.run.client}</Typography.BodySmall>
            </span>
            <RunClock page={page} />
          </div>
          {page.requests.length > 1 && (
            <div className={styles.spread}>
              <Typography.BodySmall variant="secondary">
                Request {index + 1} of {page.requests.length} for this run
              </Typography.BodySmall>
              <span className={styles.row}>
                <Button.TextButton
                  variant="bare"
                  size="32"
                  disabled={!prev}
                  onClick={() => prev && select(prev.name)}
                >
                  Previous
                </Button.TextButton>
                <Button.TextButton
                  variant="bare"
                  size="32"
                  disabled={!next}
                  onClick={() => next && select(next.name)}
                >
                  Next
                </Button.TextButton>
              </span>
            </div>
          )}
        </div>
        <div className={styles.panelBody}>
          {!session.alive && <Ended />}
          {session.done ? (
            <Closing page={page} variant="inline" />
          ) : selected ? (
            <>
              <PanelHeader
                icon={selected.kind === 'approval' ? 'ShieldAlert' : 'Link'}
                tone={decision ? 'gray' : 'amber'}
                heading="h3"
                sticky={false}
                title={selected.callPath}
                details={
                  <>
                    <Typography.BodySmall variant="tertiary">
                      {selected.kind === 'approval'
                        ? 'approval request'
                        : 'account connection request'}
                    </Typography.BodySmall>
                    <Typography.BodySmall variant="tertiary">
                      Provider: {selected.providerLabel}
                    </Typography.BodySmall>
                    <ConnectionWindow page={page} request={selected} />
                  </>
                }
                status={
                  decision ? (
                    <DecisionPill decision={decision} />
                  ) : (
                    <Pill color="amber">
                      {selected.kind === 'approval' ? 'Approval required' : 'Connection required'}
                    </Pill>
                  )
                }
              />
              <DescriptionList.List>
                <DescriptionList.Row label="Run intent">{page.run.intent}</DescriptionList.Row>
                <DescriptionList.Row label="Requested by">
                  You, from {page.run.client} · {page.run.workspaceLabel}
                </DescriptionList.Row>
                <DescriptionList.Row label="Asked">
                  <Asked at={selected.askedAt} />
                </DescriptionList.Row>
              </DescriptionList.List>
              {selected.kind === 'approval' && <Arguments source={selected.arguments} />}
              {selected.kind === 'approval' && !decision && (
                <PolicyShortcut
                  page={page}
                  request={selected}
                  onReleased={() => session.decide(selected.name, 'released')}
                />
              )}
              <Disclosure title="Program">
                <CodeBlock value={page.run.program} format="typescript" framed />
              </Disclosure>
            </>
          ) : null}
        </div>
        {selected && !decision && session.alive && !session.done && (
          <div className={styles.panelFooterBar}>
            <ApproveAll session={session} variant="secondary" />
            <RequestButtons request={selected} session={session} />
          </div>
        )}
      </section>
    </div>
  )
}

// ---------- Variant C: the Program, with its stops ----------
// The Program is the page. Each request is a stop pinned to the line that waits; picking a
// stop moves the highlight, and the rail holds the decisions.

export function VariantC({ page }: { page: HandoffPage }) {
  const session = useDecisions(page)
  const [current, setCurrent] = useState(page.linked ?? page.requests[0]?.name)
  const stop = page.requests.find((r) => r.name === current)
  const highlight = useMemo(
    () => (stop ? { startLine: stop.line, endLine: stop.line } : null),
    [stop],
  )
  return (
    <div className={styles.stack}>
      <header className={styles.spread}>
        <div className={styles.stackTight}>
          <div className={styles.row}>
            <Typography.HeadingMedium as="h1" className={styles.runIdSmall}>
              {page.run.id}
            </Typography.HeadingMedium>
            <Typography.BodySmall variant="tertiary">
              from {page.run.client} · {page.run.workspaceLabel}
            </Typography.BodySmall>
          </div>
          <Typography.Body variant="secondary">“{page.run.intent}”</Typography.Body>
        </div>
        <RunClock page={page} long />
      </header>
      {!session.alive && <Ended />}
      <div className={styles.sourceLayout}>
        <div className={styles.sourceFrame}>
          <AnnotatedCodeBlock
            code={page.run.program}
            language="typescript"
            highlight={highlight}
            followHighlight
            revealKey={current ?? ''}
          />
        </div>
        <aside className={styles.stopRail} aria-label="Where the run stopped">
          <div className={styles.spread}>
            <Typography.BodyStrong>
              Stopped {plural(page.requests.length, 'time')}, waiting for you
            </Typography.BodyStrong>
          </div>
          {session.done && <Closing page={page} variant="card" />}
          {page.requests.map((request) => {
            const d = session.decisions[request.name]
            const isCurrent = request.name === current
            return (
              <div key={request.name} className={styles.stop[isCurrent ? 'current' : 'plain']}>
                <button
                  type="button"
                  className={styles.stopPick}
                  onClick={() => setCurrent(request.name)}
                  aria-pressed={isCurrent}
                >
                  <span className={styles.row}>
                    <span className={styles.lineTag}>line {request.line}</span>
                    <Typography.BodySmallStrong>{request.callPath}</Typography.BodySmallStrong>
                    {d && <DecisionPill decision={d} />}
                  </span>
                  <span className={styles.row}>
                    <Typography.BodySmall variant="tertiary">
                      {request.kind === 'approval'
                        ? `needs your approval · ${request.providerLabel}`
                        : `needs your ${request.providerLabel} account`}
                    </Typography.BodySmall>
                    <Asked at={request.askedAt} />
                  </span>
                </button>
                {isCurrent && request.kind === 'approval' && (
                  <Arguments source={request.arguments} />
                )}
                {isCurrent && <ConnectionWindow page={page} request={request} />}
                {!d && isCurrent && (
                  <div className={styles.spread}>
                    {request.kind === 'approval' ? (
                      <PolicyShortcut
                        page={page}
                        request={request}
                        compact
                        onReleased={() => session.decide(request.name, 'released')}
                      />
                    ) : (
                      <span />
                    )}
                    <span className={styles.cardActions}>
                      <RequestButtons request={request} session={session} size="32" />
                    </span>
                  </div>
                )}
              </div>
            )
          })}
          {session.alive && !session.done && <ApproveAll session={session} variant="secondary" />}
        </aside>
      </div>
    </div>
  )
}

// ---------- switcher ----------

const variants = [
  { key: 'A', name: 'Handoff sheet', Component: VariantA },
  { key: 'B', name: 'Split, as today', Component: VariantB },
  { key: 'C', name: 'Program with stops', Component: VariantC },
] as const

export function HandoffPrototype({ page }: { page: HandoffPage }) {
  const [params, setParams] = useSearchParams()
  const key = params.get('variant') ?? 'A'
  const index = Math.max(
    0,
    variants.findIndex((v) => v.key === key),
  )
  const go = (delta: number) => {
    const nextKey = variants[(index + delta + variants.length) % variants.length].key
    setParams(
      (p) => {
        p.set('variant', nextKey)
        return p
      },
      { preventScrollReset: true },
    )
  }
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null
      if (target && target.closest('input, textarea, [contenteditable="true"]')) return
      if (event.key === 'ArrowLeft') go(-1)
      if (event.key === 'ArrowRight') go(1)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  })
  const { Component, name } = variants[index]
  return (
    <>
      <Component key={variants[index].key} page={page} />
      {import.meta.env.DEV && <Switcher label={`${variants[index].key} — ${name}`} go={go} />}
    </>
  )
}

function Switcher({ label, go }: { label: ReactNode; go: (delta: number) => void }) {
  return (
    <div className={styles.switcher} role="toolbar" aria-label="Prototype variant">
      <button
        type="button"
        className={styles.switcherButton}
        onClick={() => go(-1)}
        aria-label="Previous variant"
      >
        ←
      </button>
      <span>{label}</span>
      <button
        type="button"
        className={styles.switcherButton}
        onClick={() => go(1)}
        aria-label="Next variant"
      >
        →
      </button>
    </div>
  )
}
