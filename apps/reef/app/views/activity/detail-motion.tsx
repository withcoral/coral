import { Component, createRef } from 'react'
import * as styles from './execution-source-split.css'

// Shorter than Wax's panel reveal: a reader steps through records quickly.
const DURATION = 200
const EASING = 'cubic-bezier(0.2, 0, 0, 1)'

/** The record that leaves, as it showed, and which way the cards move. */
interface Snapshot {
  card: HTMLElement
  /** Where the old record showed: at rest, or partway through a slide a new one ends. */
  from: string
  /** 1 when the new record comes after the old one in its Run, -1 when before. */
  direction: 1 | -1
}

/**
 * Slides the selected record's whole detail, header included, when the reader moves
 * between records of one Run at known positions, its Operation Invocations and
 * Program Run Events: a later one enters from below while the old one leaves upward, and an
 * earlier one the reverse. Every other change, such as the Run itself, an unknown
 * position or reduced motion, replaces it at once.
 *
 * Native View Transitions do not fit here: their snapshots draw above the page, not
 * clipped by this pane, and they hold pointer input until they end. So this copies the
 * old detail just before React replaces it, and slides that inert copy out over the
 * pane while the live detail slides in. The pane clips both, and a new selection
 * mid-slide ends the old slide at once, so navigation never waits on it.
 */
export class DetailMotion extends Component<
  {
    /** The selected record's identity. */
    record: string | undefined
    /** Its position among its Run's records, when known. */
    position: number | undefined
    children: React.ReactNode
  },
  object,
  Snapshot | null
> {
  private readonly stage = createRef<HTMLDivElement>()
  private readonly leaving = createRef<HTMLDivElement>()
  private running: Animation[] = []

  override getSnapshotBeforeUpdate(previous: DetailMotion['props']): Snapshot | null {
    const { record, position } = this.props
    const stage = this.stage.current
    if (
      !stage ||
      record === previous.record ||
      position === undefined ||
      previous.position === undefined ||
      position === previous.position ||
      typeof stage.animate !== 'function' ||
      window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
    )
      return null
    const { transform } = getComputedStyle(stage)
    const offset = transform === 'none' ? 0 : new DOMMatrixReadOnly(transform).m42
    return {
      card: copyDetail(stage),
      from: offset ? `translateY(${offset}px)` : 'translateY(0)',
      direction: position > previous.position ? 1 : -1,
    }
  }

  override componentDidUpdate(
    previous: DetailMotion['props'],
    _: object,
    snapshot: Snapshot | null,
  ) {
    if (previous.record === this.props.record) return
    this.settle()
    const stage = this.stage.current
    const leaving = this.leaving.current
    if (!snapshot || !stage || !leaving) return
    leaving.replaceChildren(snapshot.card)
    restoreScroll(snapshot.card)
    const travel = `${snapshot.direction * 100}%`
    const timing = { duration: DURATION, easing: EASING }
    const enter = stage.animate(
      [{ transform: `translateY(${travel})` }, { transform: 'translateY(0)' }],
      timing,
    )
    const leave = leaving.animate(
      [{ transform: snapshot.from }, { transform: `translateY(${snapshot.direction * -100}%)` }],
      timing,
    )
    this.running = [enter, leave]
    // A newer slide or an unmount cancels this one, which rejects its `finished`.
    enter.finished.then(() => this.settle(), ignoreCancel)
    leave.finished.catch(ignoreCancel)
  }

  override componentWillUnmount() {
    this.settle()
  }

  /** Ends any slide at once: the live detail rests in place and the copy goes. */
  private settle() {
    for (const animation of this.running) animation.cancel()
    this.running = []
    this.leaving.current?.replaceChildren()
  }

  override render() {
    return (
      <>
        <div ref={this.stage} className={styles.detailStage}>
          {this.props.children}
        </div>
        {/* Holds the leaving copy only while it slides. It is never read or focused. */}
        <div ref={this.leaving} className={styles.detailLeaving} aria-hidden inert />
      </>
    )
  }
}

const ignoreCancel = () => {}

const SCROLL = 'data-motion-scroll'

/** An inert copy of the detail in `stage` that keeps each scrolled element's position. */
function copyDetail(stage: HTMLElement) {
  const card = stage.cloneNode(true) as HTMLElement
  const originals = [stage, ...stage.querySelectorAll<HTMLElement>('*')]
  const copies = [card, ...card.querySelectorAll<HTMLElement>('*')]
  originals.forEach((original, index) => {
    const copied = copies[index]!
    // Ids must stay unique, so labels and descriptions keep naming the live detail,
    // and no copied control joins a live form or radio group.
    for (const name of ['id', 'name', 'form']) copied.removeAttribute(name)
    if (original.scrollTop || original.scrollLeft)
      copied.setAttribute(SCROLL, `${original.scrollTop},${original.scrollLeft}`)
  })
  return card
}

/** Scrolls the copy's elements to where the old detail showed them. */
function restoreScroll(card: HTMLElement) {
  for (const element of [card, ...card.querySelectorAll<HTMLElement>(`[${SCROLL}]`)]) {
    const position = element.getAttribute(SCROLL)
    if (!position) continue
    const [top, left] = position.split(',').map(Number)
    element.scrollTop = top!
    element.scrollLeft = left!
  }
}
