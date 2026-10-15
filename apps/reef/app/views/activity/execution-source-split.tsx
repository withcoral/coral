import { useCallback, useMemo, useRef, useState } from 'react'
import { Divider } from '@/wax/components/divider'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { PanelClose, PanelInset } from './panel'
import {
  ACCEPTED_PROGRAM_DOCUMENT_ID,
  recordedLocationFor,
  sourceDocumentsFor,
  sourceLocationFor,
} from './source-location'
import { SourcePane } from './source-pane'
import type { ActivityRow, RunDetail } from './types'
import * as styles from './execution-source-split.css'

const MIN_PANE_HEIGHT = 120

/**
 * The real Activity drawer's balanced split: the selected record's own detail,
 * independently scrollable, above the Run's persistent, read-only accepted
 * source. Mount this once per Run (key it by `context.run.name`); its split
 * ratio, scroll position, and Follow selection stay put while the caller swaps
 * `children` for each record the reader selects inside that same Run, because
 * none of that state lives on the record, only on the Run.
 *
 * The split is a plain `flexGrow` ratio against `flexBasis: 0`, not a measured
 * pixel height: the browser divides whatever room `.split` actually has, at
 * every layout, including a drawer or viewport resize, with no JS measurement
 * or re-clamping needed on the read path. A drag still needs one pixel delta
 * to turn into a ratio delta, so it reads the split's current rendered height
 * on demand, from the live gesture, rather than caching it in state.
 *
 * A highlight comes from retained Coral evidence only: a failed Run's error
 * frame when the Run itself is selected (`sourceLocationFor`), or the location
 * Coral stored for the selected operation, emission or log
 * (`recordedLocationFor`). Anything else says the location is unavailable.
 */
export function ExecutionSourceSplit({
  context,
  locatable,
  selectedRow,
  close,
  children,
}: {
  context: RunDetail
  /**
   * Whether the reader selected the Run itself, not a child operation or
   * output under it. A failed Run's retained error frame names where the
   * Program threw, not where any one child call happened; attributing it to
   * a child's selection would be a fabricated location, so this must be
   * false for every record but the Run's own.
   */
  locatable: boolean
  /** The selected record, whose stored location applies when the Run itself is not selected. */
  selectedRow: ActivityRow | null
  /** The drawer's close control. The selected record's own sticky header holds it. */
  close?: React.ReactNode
  children: React.ReactNode
}) {
  const [followSelection, setFollowSelection] = useState(true)
  // The detail pane's share of the split, 0 to 1; the source pane takes the rest.
  const [splitRatio, setSplitRatio] = useState(0.5)
  const splitRef = useRef<HTMLDivElement>(null)
  // Each record's detail opens at its top; the source pane keeps its own position.
  const detailRef = useRef<HTMLDivElement>(null)
  const scrollDetailToTop = useCallback((content: HTMLDivElement | null) => {
    if (content) detailRef.current?.scrollTo?.({ top: 0 })
  }, [])
  const resizeSplit = (deltaY: number) => {
    const total = splitRef.current?.getBoundingClientRect().height
    if (!total) return
    const minRatio = MIN_PANE_HEIGHT / total
    const maxRatio = 1 - minRatio
    setSplitRatio((current) => Math.min(maxRatio, Math.max(minRatio, current + deltaY / total)))
  }

  const documents = useMemo(() => sourceDocumentsFor(context), [context])
  const verified = useMemo(
    () => (locatable ? sourceLocationFor(context) : null),
    [context, locatable],
  )
  const recorded = useMemo(
    () => (locatable || !selectedRow ? null : recordedLocationFor(context, selectedRow)),
    [context, locatable, selectedRow],
  )
  const location = verified ?? recorded
  const documentId = location?.documentId ?? ACCEPTED_PROGRAM_DOCUMENT_ID
  const document = documents.find((candidate) => candidate.id === documentId) ?? documents[0]!
  const missingLocationMessage = location ? undefined : 'Source location unavailable'

  return (
    <div className={styles.root}>
      <div className={styles.split} ref={splitRef}>
        <section
          aria-label="Selected record"
          className={styles.detailPane}
          style={{ flexGrow: splitRatio, flexBasis: 0 }}
        >
          {/* The record's header, with its close control, wraps to the pane's width. */}
          <ScrollArea
            viewportRef={detailRef}
            surface="floating"
            constrainWidth
            scrollDirection="vertical"
          >
            <div key={selectedRow?.name} ref={scrollDetailToTop} className={styles.detailContent}>
              {/* The record's own header names it and holds the close control; the split
                  adds no Run header of its own, so a Run's header shows once. */}
              <PanelInset value>
                <PanelClose value={close}>{children}</PanelClose>
              </PanelInset>
            </div>
          </ScrollArea>
        </section>
        <Divider
          label="Resize detail and source panes"
          onResize={resizeSplit}
          percent={splitRatio * 100}
        />
        <section
          aria-label="Accepted source"
          className={styles.sourcePane}
          style={{ flexGrow: 1 - splitRatio, flexBasis: 0 }}
        >
          <SourcePane
            document={document}
            highlight={location?.highlight ?? null}
            missingLocationMessage={missingLocationMessage}
            followSelection={followSelection}
            onFollowSelectionChange={setFollowSelection}
            // Each record has its own location, so selecting another one reveals it again.
            revealKey={`${context.run.name}:${selectedRow?.name ?? ''}`}
          />
        </section>
      </div>
    </div>
  )
}
