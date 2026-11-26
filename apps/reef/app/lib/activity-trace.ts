import { expandable } from '@/views/activity/navigation'
import {
  MAX_EXPANDED_RECORDS,
  type ActivityBranch,
  type ActivityPage,
  type ActivityRow,
} from '@/views/activity/types'
import { ACTIVITY_CHILD_WINDOW } from './activity-live-types'

/**
 * Run windows one request reads beside its root's. The Activity loader reads them one
 * after another, so more of them make one slow request instead of several quick ones.
 */
export const TRACE_WINDOWS_PER_REQUEST = 10

/**
 * The children of one Task, Run or tool call that the trace has read, by their ordinal. A
 * call of a private or shared tool holds the calls its body made.
 */
export interface TraceBranch {
  /** How many children Coral counts. It grows while the parent's work goes on. */
  total: number
  rows: Record<number, ActivityRow>
  unavailable: boolean
  /**
   * A read reached the end after the parent ended, so nothing here changes again. Until
   * then, later evidence can add children or end the ones that were in progress.
   */
  final: boolean
  /**
   * Where later evidence may have changed it: the first ordinal of each window to read
   * again. They cover the children that were in progress, and the end, where new ones come.
   */
  stale?: number[]
  /**
   * Windows to read once more only to check that Coral still returns them as they are. They
   * wait for every missing and stale window, so a check never delays a live update.
   */
  check?: number[]
  /**
   * A read of the current pass joined while the parent still worked. Once the parent ends,
   * one more pass reads the same windows again before the branch is final.
   */
  live?: boolean
  /** Coral withheld these children. The trace asks once more whether it still does. */
  retry?: boolean
}

/**
 * Everything read so far under one top-level record: a Task with its tool calls and Runs,
 * or a standalone Run, each Run's tool calls and Events, and the calls each tool's body
 * made. The Activity list holds only bounded windows of the records it expands. A waterfall
 * needs every record, so this store reads the rest through the same loader, window by
 * window.
 */
export interface ActivityTraceStore {
  root: string
  rootRow: ActivityRow | null
  /**
   * What the root Task's own detail said, once a read of it, or a page that selects the
   * Task, brings it: its feedback, or none. Undefined until then.
   */
  taskDetail?: { feedback?: string }
  branches: Record<string, TraceBranch>
  /** Coral did not return the root, or its children. */
  unavailable: boolean
  /** The last read failed. Nothing more loads until a retry. */
  failed: boolean
  /** Coral withheld the root. The trace asks once more whether it still does. */
  retry?: boolean
  /**
   * The next request reads the windows of a check before anything else. While a check has
   * windows to read, every second request does: later evidence that keeps coming never
   * holds a check back, and a check never takes two requests in a row from live updates.
   */
  checkFirst?: boolean
  /**
   * The last parent a request read for later evidence. The next request starts after it.
   * A request holds `TRACE_WINDOWS_PER_REQUEST` windows, so when more parents change than
   * that, each has its turn, and none waits for the same first ones again and again.
   */
  served?: string
  /**
   * For each parent, the window a request last read for later evidence. The next request
   * reads the parent's next changeable window, so evidence that marks them all again never
   * keeps the later ones waiting behind the first.
   */
  turns?: Record<string, number>
  /**
   * How many times the trace has learned that Coral withholds something. A request carries
   * the count from when it left. An answer to a request that left before the trace learned
   * of a withholding is older than that news: it never returns what the trace holds as
   * withheld.
   */
  learned?: number
  /** For each parent the trace holds as withheld, `learned` when it was last told so. */
  toldAt?: Record<string, number>
}

/** A child ordinal the reader needs next, such as one beside the rows on screen. */
export interface TracePreference {
  parent: string
  at: number
}

export interface TraceRequest {
  windows: { parent: string; offset: number }[]
  /** The last parent the request reads for later evidence. The trace keeps it as its turn. */
  served?: string
  /** What the trace had learned of withholdings when the request left. */
  learned?: number
  /** Also reads the root Task's own detail, once: its header states its feedback. */
  taskDetail?: true
}

export function openTrace(root: string): ActivityTraceStore {
  return {
    root,
    rootRow: null,
    branches: {},
    unavailable: false,
    failed: false,
  }
}

/**
 * A trace of `row` alone, for a record whose top-level record the trace does not hold.
 * While the read that looks for it is `pending`, its history is loading. Once Coral does
 * not return it, its history is unavailable.
 */
export function loneTrace(row: ActivityRow, pending: boolean): ActivityTraceStore {
  return {
    ...openTrace(row.name),
    rootRow: row,
    branches: pending ? {} : { [row.name]: withheld() },
  }
}

type RunRow = Extract<ActivityRow, { kind: 'run' }>
type CallRow = Extract<ActivityRow, { kind: 'invocation' }>

/** The loaded children of `parent`, in Coral's order. */
export function traceChildren(store: ActivityTraceStore, parent: string): ActivityRow[] {
  const branch = store.branches[parent]
  if (!branch) return []
  return Object.keys(branch.rows)
    .map(Number)
    .toSorted((a, b) => a - b)
    .map((position) => branch.rows[position]!)
}

/** The Runs the trace knows: the root itself, or the Runs among a Task's loaded children. */
export function traceRuns(store: ActivityTraceStore): RunRow[] {
  if (store.rootRow?.kind === 'run') return [store.rootRow]
  return traceChildren(store, store.root).filter((row): row is RunRow => row.kind === 'run')
}

const indexes = new WeakMap<ActivityTraceStore['branches'], Map<string, ActivityRow>>()
/** Every record the trace holds under its root, by name. */
function traceRows(store: ActivityTraceStore) {
  let index = indexes.get(store.branches)
  if (!index) {
    index = new Map()
    for (const branch of Object.values(store.branches))
      for (const row of Object.values(branch.rows)) index.set(row.name, row)
    indexes.set(store.branches, index)
  }
  return index
}

/** The record named `name` among the children the trace holds. */
export const traceRecord = (store: ActivityTraceStore, name: string) => traceRows(store).get(name)

/**
 * Every tool call the trace holds under `holder`, a Run or a tool call: its own calls in
 * Coral's order, each followed by the calls its body made.
 */
export function traceCalls(store: ActivityTraceStore, holder: string): CallRow[] {
  return traceChildren(store, holder).flatMap((row) =>
    row.kind === 'invocation' ? [row, ...(expandable(row) ? traceCalls(store, row.name) : [])] : [],
  )
}

/**
 * The records whose children the trace reads: the root, each Run under a Task, and each
 * call of a tool whose body can make calls.
 */
function traceParents(store: ActivityTraceStore): string[] {
  let parents = parentLists.get(store)
  if (!parents) {
    const runs = traceRuns(store)
    parents = [
      store.root,
      ...(store.rootRow?.kind === 'run' ? [] : runs.map((run) => run.name)),
      ...runs.flatMap((run) =>
        traceCalls(store, run.name)
          .filter(expandable)
          .map((call) => call.name),
      ),
    ]
    parentLists.set(store, parents)
  }
  return parents
}
const parentLists = new WeakMap<ActivityTraceStore, string[]>()

/** The spans of ordinals under `parent` that no read has returned yet, as [from, to). */
export function missingWindows(branch: TraceBranch | undefined): { from: number; to: number }[] {
  if (!branch || branch.unavailable) return []
  const gaps: { from: number; to: number }[] = []
  let from = -1
  for (let position = 0; position < branch.total; position++) {
    const loaded = position in branch.rows
    if (!loaded && from < 0) from = position
    if (loaded && from >= 0) {
      gaps.push({ from, to: position })
      from = -1
    }
  }
  if (from >= 0) gaps.push({ from, to: branch.total })
  return gaps
}

/**
 * The windows one request needs to read the children of `parent` from `offset`: its own
 * and, for a tool call, a window of each record around it that shows the call inside it.
 * The loader opens a tool call only in a window that shows the call. Null when the trace
 * does not hold the call's place.
 */
function windowChain(store: ActivityTraceStore, parent: string, offset: number) {
  const chain = [{ parent, at: offset }]
  let row = traceRows(store).get(parent)
  while (row?.kind === 'invocation') {
    if (row.position === undefined || chain.length > MAX_EXPANDED_RECORDS) return null
    const holder: string = row.parent?.invocation ?? row.run
    chain.push({ parent: holder, at: row.position })
    row = holder === store.root ? undefined : traceRows(store).get(holder)
  }
  return chain
}

/**
 * Whether one request can read the children of `parent`. The loader expands at most
 * `MAX_EXPANDED_RECORDS` records for a request, and a tool call needs every record around
 * it open, so the calls of a tool call nested deeper than that are not readable.
 */
export function traceReadable(store: ActivityTraceStore, parent: string) {
  const chain = windowChain(store, parent, 0)
  if (!chain) return false
  const beside = chain.some((item) => item.parent === store.root) ? 0 : 1
  return chain.length + beside <= MAX_EXPANDED_RECORDS
}

/** Whether `row`'s own record says its work is over, so its children change no more. */
function parentEnded(row: ActivityRow | null | undefined) {
  if (row?.kind === 'run') return row.state !== 'running' && row.state !== 'unknown'
  // A Task's end never ends its Runs: `contextImpactPartial` says one still runs.
  if (row?.kind === 'task') return Boolean(row.endedAt) && !row.contextImpactPartial
  if (row?.kind === 'invocation') return row.state === 'ended'
  return false
}

/** Whether a child's own record can still change: a Run or a tool call in progress. */
function inProgress(row: ActivityRow) {
  if (row.kind === 'run') return row.state === 'running' || row.state === 'unknown'
  if (row.kind === 'invocation') return row.state !== 'ended'
  if (row.kind === 'tool') return !row.completedAt && row.outcome !== 'unknown'
  return false
}

function parentRow(store: ActivityTraceStore, parent: string) {
  if (parent === store.root) return store.rootRow
  return traceRows(store).get(parent)
}

/** The window starts that cover `ordinals`, each window as far along as it can start. */
function windowStarts(ordinals: number[]) {
  const starts: number[] = []
  for (const ordinal of ordinals.toSorted((a, b) => a - b))
    if (!starts.length || ordinal >= starts.at(-1)! + ACTIVITY_CHILD_WINDOW) starts.push(ordinal)
  return starts
}

/** The children of `branch` whose own record can still change, by ordinal. */
const unsettled = (branch: TraceBranch) =>
  Object.keys(branch.rows)
    .map(Number)
    .filter((position) => inProgress(branch.rows[position]!))

/**
 * The windows later evidence can change: one for each child in progress, and one at the
 * end, where new children come. A child that waits early in a long Run costs its own
 * window, never every window after it.
 */
const staleWindows = (branch: TraceBranch) => windowStarts([...unsettled(branch), branch.total])

function sameRow(a: ActivityRow | undefined, b: ActivityRow) {
  return a === b || (a !== undefined && JSON.stringify(a) === JSON.stringify(b))
}

/**
 * The ordinals [from, to) one read covers. The loader also adds a Run from outside a Task's
 * window to that Task's rows, so the number of rows is not the length of the window.
 */
function readWindow(read: ActivityBranch) {
  const from = read.offset ?? 0
  const positions = new Set(read.rows.map((row, index) => row.position ?? from + index))
  let to = from
  while (positions.has(to)) to++
  return { from, to }
}

const withheld = (): TraceBranch => ({ total: 0, rows: {}, unavailable: true, final: true })

/**
 * `branch` with one read of its parent's children joined in. The same object when nothing
 * changed. `asked` is the ordinal the read was asked to start at, when the trace asked.
 */
function joinBranch(
  current: TraceBranch | undefined,
  read: ActivityBranch,
  ended: boolean,
  asked?: number,
): TraceBranch {
  // Coral answers that you cannot read these children. What an earlier read returned goes
  // too: a record you may no longer read never stays on screen.
  if (read.unavailable)
    return current?.unavailable && !current.total && !current.retry ? current : withheld()
  const window = readWindow(read)
  const readTotal = Number(read.total ?? 0)
  const position = (row: ActivityRow, index: number) => row.position ?? window.from + index
  const held = current && !current.unavailable ? current : undefined
  // An ordinal the branch holds names another record now: Coral's history under this
  // parent changed, as when it stops returning one child and the later ones move up.
  const moved =
    held !== undefined &&
    read.rows.some((row, index) => {
      const have = held.rows[position(row, index)]
      return have !== undefined && have.name !== row.name
    })
  if (asked === undefined && held) {
    // A page the trace did not ask for can be an older snapshot. It never replaces a record
    // with another one, and it never changes a final branch. Where it disagrees, the trace
    // asks Coral itself, and that answer decides.
    const doubt = () => {
      const stale = windowStarts([...(held.stale ?? []), window.from])
      return JSON.stringify(stale) === JSON.stringify(held.stale) ? held : { ...held, stale }
    }
    if (moved) return doubt()
    if (held.final && readTotal <= held.total) {
      const same =
        readTotal === held.total &&
        read.rows.every((row, index) => sameRow(held.rows[position(row, index)], row))
      return same ? held : doubt()
    }
  }
  // The answer to the trace's own read is Coral's word now. One that counts fewer children
  // than the branch holds, at its end, says that the last ones are gone.
  const shrank =
    asked !== undefined && held !== undefined && window.to >= readTotal && readTotal < held.total
  let rows = current?.rows ?? {}
  let changed = !current || current.unavailable
  if (asked !== undefined && held && (moved || shrank)) {
    // Every ordinal outside the answer can have moved too: only the answer's records stay,
    // and the trace reads the rest again. After a count that only shrank, the ordinals
    // before the answer are as they were.
    changed = true
    rows = {}
    if (!moved)
      for (const [at, row] of Object.entries(held.rows))
        if (Number(at) < Math.min(window.from, readTotal)) rows[Number(at)] = row
    read.rows.forEach((row, index) => (rows[position(row, index)] = row))
  } else
    read.rows.forEach((row, index) => {
      const at = position(row, index)
      if (sameRow(rows[at], row)) return
      if (!changed) changed = true
      if (rows === current?.rows) rows = { ...rows }
      rows[at] = row
    })
  const reset = asked !== undefined && held !== undefined && (moved || shrank)
  // One at a time: a long Run has more children than a call takes arguments.
  const loadedEnd = Object.keys(rows).reduce((end, at) => Math.max(end, Number(at) + 1), 0)
  // Coral only adds children by itself, and an older snapshot can join after a newer read.
  const total = reset
    ? Math.max(readTotal, loadedEnd)
    : Math.max(readTotal, loadedEnd, held?.total ?? 0)
  const reachedEnd = window.to >= total
  // The read settles the windows that start inside it, and every later one at the end.
  const rest = (starts: number[] | undefined) =>
    reset
      ? undefined
      : starts?.filter(
          (at) => at !== asked && !(window.from <= at && (at < window.to || reachedEnd)),
        )
  const check = rest(current?.check)
  // A final branch that its own read confirms stays final, with one check fewer.
  if (held?.final && asked !== undefined && !changed && total === held.total) {
    const stale = rest(held.stale)
    const next: TraceBranch = { ...held }
    if (stale?.length) next.stale = stale
    else delete next.stale
    if (check?.length) next.check = check
    else delete next.check
    return JSON.stringify(next) === JSON.stringify(held) ? held : next
  }
  const branch: TraceBranch = { total, rows, unavailable: false, final: false }
  let stale = rest(current?.stale)
  let live = Boolean(current?.live) || !ended
  // This read alone holds everything that could still change, from the first child that
  // was in progress to the end.
  const settles = reachedEnd && window.from <= (held ? Math.min(...unsettled(held), held.total) : 0)
  if (ended && !missingWindows(branch).length && (settles || !stale?.length)) {
    // Every read of what could change came after the parent ended: nothing changes again.
    if (settles || !live) {
      branch.final = true
      stale = undefined
      live = false
    } else {
      stale = staleWindows(branch)
      live = false
    }
  }
  if (stale?.length) branch.stale = stale
  if (check?.length) branch.check = check
  if (live) branch.live = true
  if (
    !changed &&
    current &&
    current.total === total &&
    current.final === branch.final &&
    Boolean(current.live) === live &&
    JSON.stringify(current.stale ?? []) === JSON.stringify(branch.stale ?? []) &&
    JSON.stringify(current.check ?? []) === JSON.stringify(branch.check ?? [])
  )
    return current
  return branch
}

/** Whether `branch`, the children of `parent`, belongs to the trace. */
function belongs(store: ActivityTraceStore, parent: string, branch: ActivityBranch) {
  if (parent === store.root) return true
  const row = traceRows(store).get(parent)
  if (row?.kind === 'run' || (row?.kind === 'invocation' && expandable(row))) return true
  return branch.rows.some(
    (child) =>
      (child.kind === 'invocation' || child.kind === 'output') &&
      (child.task === store.root || child.run === store.root),
  )
}

/**
 * Drops what the trace holds under a record it no longer holds. Coral withholds a Run or a
 * tool call by withholding its children, so what an earlier read returned under them goes
 * too: when Coral returns the record again, the trace reads them again from the start.
 */
function prune(store: ActivityTraceStore): ActivityTraceStore {
  const held = new Set([store.root])
  const visit = (parent: string) => {
    for (const row of Object.values(store.branches[parent]?.rows ?? {})) {
      if (held.has(row.name)) continue
      if (row.kind !== 'run' && !(row.kind === 'invocation' && expandable(row))) continue
      held.add(row.name)
      visit(row.name)
    }
  }
  visit(store.root)
  const orphans = Object.keys(store.branches).filter((parent) => !held.has(parent))
  if (!orphans.length) return store
  const branches = Object.fromEntries(
    Object.entries(store.branches).filter(([parent]) => held.has(parent)),
  )
  return { ...store, branches }
}

/**
 * Closes the branches whose parent has ended. A branch read while its parent still worked
 * gets one more pass over what could change. A branch with no such read left is final.
 */
function settle(store: ActivityTraceStore): ActivityTraceStore {
  let branches = store.branches
  for (const [parent, branch] of Object.entries(store.branches)) {
    if (branch.final || branch.unavailable || branch.stale?.length) continue
    if (missingWindows(branch).length || !parentEnded(parentRow(store, parent))) continue
    if (branches === store.branches) branches = { ...branches }
    branches[parent] = branch.live
      ? { ...branch, stale: staleWindows(branch), live: false }
      : { ...branch, final: true }
  }
  return branches === store.branches ? store : { ...store, branches }
}

/**
 * Joins what a page shows of the trace: the root's row and the windows of its expanded
 * records. The Activity list's own reads and live snapshots go through here too, so the
 * trace never reads again what the list already holds. Returns the same store when the
 * page adds nothing.
 */
export function absorbPage(
  store: ActivityTraceStore,
  page: ActivityPage,
  asked?: ReadonlyMap<string, number>,
): ActivityTraceStore {
  // A page the trace did not ask for is a snapshot of the list: what it withholds, it
  // withholds now.
  const told = asked
    ? []
    : Object.keys(page.branches).filter((parent) => page.branches[parent]!.unavailable)
  return learn(store, joinPage(store, page, asked), told)
}

/**
 * `next` with what the trace has learned that Coral withholds, after `store`: the count,
 * and for each withheld parent the count at which the trace was last told. `told` names the
 * parents a snapshot of the list states as withheld now: it is news also when the trace
 * holds them as withheld already, because it is later than a read that left before it.
 */
function learn(
  store: ActivityTraceStore,
  next: ActivityTraceStore,
  told: readonly string[] = [],
): ActivityTraceStore {
  if (next === store && !told.length) return store
  const news = new Set(told.filter((parent) => next.branches[parent]?.unavailable))
  for (const [parent, branch] of Object.entries(next.branches))
    if (branch.unavailable && !store.branches[parent]?.unavailable) news.add(parent)
  if (!news.size && !(next.unavailable && !store.unavailable)) return next
  const learned = (store.learned ?? 0) + 1
  // Only what is withheld now has a count to compare with.
  const toldAt: Record<string, number> = {}
  for (const [parent, branch] of Object.entries(next.branches))
    if (branch.unavailable)
      toldAt[parent] = news.has(parent) ? learned : (store.toldAt?.[parent] ?? 0)
  return { ...next, learned, toldAt }
}

function joinPage(
  store: ActivityTraceStore,
  page: ActivityPage,
  asked?: ReadonlyMap<string, number>,
): ActivityTraceStore {
  const listed =
    page.rows.find((row) => row.name === store.root) ??
    (page.selectedRow?.name === store.root ? page.selectedRow : undefined)
  let next = store
  const change = (patch: Partial<ActivityTraceStore>) => {
    next = { ...next, ...patch }
  }
  if (listed && !sameRow(store.rootRow ?? undefined, listed)) change({ rootRow: listed })
  // A page that selects the root Task holds its detail, and so its feedback.
  if (
    page.detail?.kind === 'task' &&
    page.detail.task.name === store.root &&
    (!store.taskDetail || store.taskDetail.feedback !== page.detail.feedback)
  )
    change({ taskDetail: { feedback: page.detail.feedback } })
  // Coral returns the root again after it withheld it: the trace reads it from the start.
  if (listed && store.unavailable) change({ unavailable: false, retry: undefined })
  // The root's own children first, then each Run's: they name the records whose windows
  // follow. A tool call's window joins once the window that holds the call has.
  const rank = (name: string) => (name === store.root ? 0 : /\/runs\/[^/]+$/.test(name) ? 1 : 2)
  let waiting = Object.keys(page.branches).toSorted((a, b) => rank(a) - rank(b))
  for (let joining = true; joining && waiting.length;) {
    joining = false
    const skipped: string[] = []
    for (const parent of waiting) {
      const read = page.branches[parent]!
      if (!belongs(next, parent, read)) {
        skipped.push(parent)
        continue
      }
      joining = true
      const joined = joinBranch(
        next.branches[parent],
        read,
        parentEnded(parentRow(next, parent)),
        asked?.get(parent),
      )
      if (joined !== next.branches[parent])
        change({ branches: { ...next.branches, [parent]: joined } })
    }
    waiting = skipped
  }
  return settle(prune(next))
}

/** Whether the window that starts at `at` holds the ordinal `position`. */
const covers = (at: number | undefined, position: number) =>
  at !== undefined && at <= position && position < at + ACTIVITY_CHILD_WINDOW

/**
 * The next read the trace needs, or null when it holds everything it can. In order: the
 * root's children, each Run's, each tool call's, then the windows later evidence made
 * stale. A parent `prefer` names goes first, and its read starts where `prefer` says, so
 * the children beside the rows on screen never wait for the rest.
 */
export function nextTraceRequest(
  store: ActivityTraceStore,
  prefer: readonly TracePreference[] = [],
): TraceRequest | null {
  if (store.failed) return null
  // A root that Coral withheld: asked for again only when a revalidation says so.
  const left: Pick<TraceRequest, 'learned' | 'taskDetail'> = store.learned
    ? { learned: store.learned }
    : {}
  // A Task's own detail rides on the next read, once: no read but this one asks for it.
  if (store.rootRow?.kind === 'task' && !store.taskDetail && !store.unavailable)
    left.taskDetail = true
  if (store.unavailable)
    return store.retry ? { windows: [{ parent: store.root, offset: 0 }], ...left } : null
  const rootBranch = store.branches[store.root]
  if (!rootBranch) return { windows: [{ parent: store.root, offset: 0 }], ...left }
  const wanted = new Map<string, number>()
  const want = (parent: string, tier: 'missing' | 'changed' | 'check') => {
    if (wanted.has(parent)) return
    const branch = store.branches[parent]
    // Children that Coral withheld: asked for again only when a revalidation says so.
    if (branch?.unavailable) return void (branch.retry && tier === 'check' && wanted.set(parent, 0))
    if (tier === 'check')
      return void (branch?.check?.length && wanted.set(parent, branch.check[0]!))
    if (!branch) return void wanted.set(parent, 0)
    const gaps = missingWindows(branch)
    const at = prefer.find((item) => item.parent === parent)?.at
    const near = at === undefined ? undefined : gaps.find((gap) => gap.from <= at && at < gap.to)
    // A window that holds the preferred ordinal, and as much of its gap as it can.
    if (near && at !== undefined)
      return void wanted.set(
        parent,
        Math.max(near.from, Math.min(at, near.to - ACTIVITY_CHILD_WINDOW)),
      )
    if (gaps[0]) return void wanted.set(parent, gaps[0].from)
    if (tier !== 'changed' || !branch.stale?.length) return
    // The window after the one read last, so each changeable window has its turn.
    const last = store.turns?.[parent]
    const next = last === undefined ? undefined : branch.stale.find((start) => start > last)
    wanted.set(parent, next ?? branch.stale[0]!)
  }
  const parents = traceParents(store).filter((parent) => traceReadable(store, parent))
  const order = [
    ...prefer.map((item) => item.parent).filter((name) => parents.includes(name)),
    ...parents,
  ]
  // Missing children first, so a stale tail never delays the first full read. Then what
  // later evidence changed. Last, the windows that are only checked: a check of a long
  // history never comes before the tail of a Run that runs. One request in two takes the
  // checks first instead, so evidence that never stops cannot hold a check back for good.
  const tiers = store.checkFirst
    ? (['check', 'missing', 'changed'] as const)
    : (['missing', 'changed', 'check'] as const)
  // What evidence changed is read in turn, from the parent after the one read last.
  const turn = parents.indexOf(store.served ?? '') + 1
  const inTurn = [
    ...order.slice(0, order.length - parents.length),
    ...parents.slice(turn),
    ...parents.slice(0, turn),
  ]
  const changed = new Set<string>()
  for (const tier of tiers)
    for (const parent of tier === 'changed' ? inTurn : order) {
      const before = wanted.size
      want(parent, tier)
      if (tier === 'changed' && wanted.size > before) changed.add(parent)
    }
  // Every window is read: the Task's detail, if still owed, is a read of its own.
  if (!wanted.size) return left.taskDetail ? { windows: [], ...left } : null
  let root = wanted.get(store.root)
  const windows = new Map<string, number>()
  /**
   * Adds the window of `parent` at `offset`, with the windows that show a tool call to the
   * loader. A request holds `TRACE_WINDOWS_PER_REQUEST` windows, and the first one's whole
   * chain however long it is, so a deep call never waits for a request with room for it.
   */
  const add = (parent: string, offset: number) => {
    const chain = windowChain(store, parent, offset)
    if (!chain) return false
    const fresh: typeof chain = []
    let rootAt = root
    for (const [index, item] of chain.entries()) {
      if (item.parent === store.root) {
        if (rootAt === undefined) rootAt = item.at
        else if (!covers(rootAt, item.at)) return false
      } else if (windows.has(item.parent)) {
        const open = windows.get(item.parent)
        if (index === 0 ? open !== item.at : !covers(open, item.at)) return false
      } else fresh.push(item)
    }
    if (windows.size && windows.size + fresh.length > TRACE_WINDOWS_PER_REQUEST) return false
    root = rootAt
    // Outermost first, as the loader opens them.
    for (const item of fresh.toReversed()) windows.set(item.parent, item.at)
    return true
  }
  let served: string | undefined
  for (const [parent, offset] of wanted)
    if (parent !== store.root && add(parent, offset) && changed.has(parent)) served = parent
  // The loader finds a Run through its Task's window, so every request reads the root's.
  return {
    windows: [
      { parent: store.root, offset: root ?? windowStart(rootBranch) },
      ...[...windows].map(([parent, offset]) => ({ parent, offset })),
    ],
    ...(served ? { served } : {}),
    ...left,
  }
}

/** Where a re-read of an up-to-date branch starts: its last window. */
const windowStart = (branch: TraceBranch) => Math.max(0, branch.total - ACTIVITY_CHILD_WINDOW)

/**
 * The Activity loader's query for `request`. It names only identities and ordinals; the
 * loader authorizes every record again.
 */
export function traceQuery(store: ActivityTraceStore, request: TraceRequest): URLSearchParams {
  const query = new URLSearchParams({ tree: '1', anchor: store.root })
  // The loader reads the Task's detail for the record the query selects.
  if (request.taskDetail) query.set('record', store.root)
  for (const { parent, offset } of request.windows) {
    query.append('expand', parent)
    query.set(`offset.${parent}`, String(offset))
    query.set(`preceding.${parent}`, '0')
  }
  return query
}

/**
 * Joins the answer to `request`. A null page is a read that failed: the trace keeps what
 * it holds and reads no more until a retry. An answer in which Coral denies the list, or no
 * longer returns the root, a Run or a tool call's calls, is a different thing: you may not
 * read it now, so its records go.
 *
 * `newer` is a snapshot of the list that came after the request left. Where the answer
 * withholds the root, or children, that this snapshot shows readable, nothing says which
 * of the two is right now. The records still go, and the trace asks Coral once more: that
 * answer decides, unless a still later snapshot of the list shows them first, since Coral
 * authorizes each list page too.
 */
export function receiveTrace(
  store: ActivityTraceStore,
  request: TraceRequest,
  page: ActivityPage | null,
  newer?: Pick<ActivityPage, 'branches'> & Partial<Pick<ActivityPage, 'rows' | 'selectedRow'>>,
): ActivityTraceStore {
  if (!page) return { ...store, failed: true }
  // The loader says so only when Coral denies the list or does not find it. Every request
  // names the root as its anchor, so Coral returns it when you can read it.
  if (
    page.listUnavailable ||
    page.anchorUnavailable ||
    !page.rows.some((row) => row.name === store.root)
  ) {
    // Withheld by this answer, listed in a later snapshot: ask once more. Only a root that
    // this answer withheld, so Coral's next answer is final.
    const listed =
      !store.unavailable &&
      Boolean(
        newer?.rows?.some((row) => row.name === store.root) ||
        newer?.selectedRow?.name === store.root,
      )
    return learn(store, { ...openTrace(store.root), unavailable: true, retry: listed || undefined })
  }
  // The request left before the trace was last told that Coral withholds something, so the
  // answer can be from before that. What the trace was told since stays withheld.
  const left = request.learned ?? 0
  if (left < (store.learned ?? 0) && store.unavailable) return store
  const asked = new Map(request.windows.map(({ parent, offset }) => [parent, offset]))
  let next = joinPage(store, page, asked)
  // The Task's detail was read: what it says stands, also that it has no feedback.
  if (request.taskDetail && !next.taskDetail) next = { ...next, taskDetail: {} }
  const branches = { ...next.branches }
  for (const [parent, branch] of Object.entries(store.branches))
    if (branch.unavailable && branches[parent] && left < (store.toldAt?.[parent] ?? 0))
      branches[parent] = branch
  for (const { parent, offset } of request.windows) {
    const branch = branches[parent]
    // Coral returned no window for a record it was asked for: it withholds its children.
    if (!page.branches[parent]) branches[parent] = withheld()
    // The answer does not hold the children it was asked for. Asking again would loop, so
    // the branch keeps what it has and says the rest is unavailable.
    else if (
      branch &&
      !branch.unavailable &&
      missingWindows(branch).some((gap) => gap.from <= offset && offset < gap.to)
    )
      branches[parent] = {
        ...branch,
        unavailable: true,
        final: true,
        stale: undefined,
        check: undefined,
        retry: undefined,
      }
  }
  // Withheld by this answer, readable in a later snapshot: ask once more. Only a branch that
  // this answer withheld, not one it found withheld, so Coral's next answer is final.
  for (const { parent } of request.windows) {
    const branch = branches[parent]
    const shown = newer?.branches[parent]
    if (branch?.unavailable && !store.branches[parent]?.unavailable && shown && !shown.unavailable)
      branches[parent] = { ...branch, retry: true }
  }
  const joined = settle(prune({ ...next, branches }))
  // After a request that took what changed first, the next takes the check's windows first.
  const checkFirst = !store.checkFirst && checking(joined)
  const turned = checkFirst ? { ...joined, checkFirst } : withoutCheckFirst(joined)
  // The windows this request read for later evidence are each parent's turn now.
  let turns = store.turns
  for (const { parent, offset } of request.windows)
    if (store.branches[parent]?.stale?.includes(offset) && turns?.[parent] !== offset)
      turns = { ...turns, [parent]: offset }
  const served = request.served ?? store.served
  return learn(store, {
    ...turned,
    ...(served ? { served } : {}),
    ...(turns ? { turns } : {}),
  })
}

function withoutCheckFirst(store: ActivityTraceStore): ActivityTraceStore {
  if (!store.checkFirst) return store
  const { checkFirst: _turn, ...rest } = store
  return rest
}

/**
 * Whether a check still has windows that a request can read, or the trace still asks once
 * more for children Coral withheld. Either takes every second request, so evidence that
 * marks more changing parents than one request holds never keeps them waiting.
 */
const checking = (store: ActivityTraceStore) =>
  traceParents(store).some((parent) => {
    const branch = store.branches[parent]
    return (branch?.check?.length || branch?.retry) && traceReadable(store, parent)
  })

/**
 * Marks the parts of the trace that later evidence may have changed, so the next reads
 * fetch only those: for each parent that is not final, the windows of its children in
 * progress and its end. A parent in `covered` was just read by the Activity list itself.
 * Returns the same store when everything is final.
 */
export function markStale(
  store: ActivityTraceStore,
  covered: ReadonlySet<string> = new Set(),
): ActivityTraceStore {
  let branches = store.branches
  for (const [parent, branch] of Object.entries(store.branches)) {
    if (branch.final || branch.unavailable || covered.has(parent)) continue
    const stale = windowStarts([...(branch.stale ?? []), ...staleWindows(branch)])
    // A pass that starts after the parent ended is the last one.
    const live = !parentEnded(parentRow(store, parent))
    if (JSON.stringify(stale) === JSON.stringify(branch.stale) && live === Boolean(branch.live))
      continue
    if (branches === store.branches) branches = { ...branches }
    branches[parent] = { ...branch, stale, live }
  }
  return branches === store.branches ? store : { ...store, branches }
}

/**
 * Marks everything the trace holds to be read once more, and everything Coral withheld to
 * be asked for once more. Nothing under an ended parent changes by itself, but Coral can
 * stop returning a record: your access can end, and history is deleted. It can also return
 * one again. What the trace holds stays on screen while the reads run, and what Coral
 * withheld stays unavailable. Each answer takes effect when it arrives. While an earlier
 * check still has windows to read, it returns the same store: that check ends first.
 */
export function revalidateTrace(store: ActivityTraceStore): ActivityTraceStore {
  if (store.unavailable) return store.retry ? store : { ...store, retry: true }
  // The check before this one still has windows to read: it goes on. To start over would
  // read the first windows again and again, and never the last ones of a long history.
  if (checking(store)) return store
  let branches = store.branches
  for (const [parent, branch] of Object.entries(store.branches)) {
    if (branches === store.branches) branches = { ...branches }
    if (branch.unavailable) {
      branches[parent] = { ...branch, retry: true }
      continue
    }
    const starts = [0]
    while (starts.at(-1)! + ACTIVITY_CHILD_WINDOW < branch.total)
      starts.push(starts.at(-1)! + ACTIVITY_CHILD_WINDOW)
    branches[parent] = { ...branch, check: starts }
  }
  return branches === store.branches ? store : { ...store, branches }
}

/** The parents whose whole changeable part `page` holds, so no extra read is needed. */
export function coveredBy(store: ActivityTraceStore, page: ActivityPage): Set<string> {
  const covered = new Set<string>()
  for (const [parent, read] of Object.entries(page.branches)) {
    const branch = store.branches[parent]
    if (!branch || read.unavailable) continue
    const window = readWindow(read)
    if (window.from <= Math.min(...unsettled(branch), branch.total) && window.to >= branch.total)
      covered.add(parent)
  }
  return covered
}

/** How much of the trace is read. */
export interface TraceProgress {
  /** Every child of the root, of each of its Runs and of each tool call is read. */
  complete: boolean
  /**
   * A record has ended, and no read after its end holds its children yet. What the trace
   * holds of them can be from before the end.
   */
  settling: boolean
  /** Nothing read can change again: the root and every Run have ended. */
  final: boolean
  /** A Task's or Run's children are not readable, so the trace has a hole no read fills. */
  unavailable: boolean
  /** Later evidence changed a part that no read has fetched again yet. */
  stale: boolean
  loaded: number
  total: number
}

export function traceProgress(store: ActivityTraceStore): TraceProgress {
  const parents = traceParents(store)
  let settling = false
  let loaded = 0
  let total = 0
  let complete = Boolean(store.rootRow)
  let final = true
  let unavailable = store.unavailable
  let stale = false
  for (const parent of parents) {
    const branch = store.branches[parent]
    if (!branch) {
      complete = false
      final = false
      if (!traceReadable(store, parent)) unavailable = true
      continue
    }
    if (branch.unavailable) unavailable = true
    if (branch.stale?.length || branch.check?.length) stale = true
    loaded += Object.keys(branch.rows).length
    total += branch.total
    if (missingWindows(branch).length) complete = false
    if (!branch.final) {
      final = false
      if (!branch.unavailable && parentEnded(parentRow(store, parent))) settling = true
    }
  }
  return {
    complete: complete && !unavailable,
    settling,
    final: final && complete,
    unavailable,
    stale,
    loaded,
    total,
  }
}
