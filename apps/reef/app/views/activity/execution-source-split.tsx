import { useLayoutEffect, useMemo, useRef, useState } from 'react'
import * as Card from '@/wax/components/card'
import { Divider } from '@/wax/components/divider'
import { Container as ScrollArea } from '@/wax/components/scroll-area'
import { DetailMotion } from './detail-motion'
import { PanelClose, PanelFooterSlot, PanelInset, PanelInsetLabel } from './panel'
import {
  ACCEPTED_PROGRAM_DOCUMENT_ID,
  madeByCall,
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
 * ratio and the source's DOM and scroll position stay put while the caller swaps
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
 * Coral stored for the selected tool, emission or log
 * (`recordedLocationFor`). Each newly selected record's line is revealed. Only
 * a tool, emission or log says its location is unavailable when it has
 * none: the Run itself and its other events have no call of their own.
 */
export function ExecutionSourceSplit({
  context,
  locatable,
  selectedRow,
  position,
  close,
  children,
}: {
  context: RunDetail
  /**
   * Whether the reader selected the Run itself, not a child tool or
   * output under it. A failed Run's retained error frame names where the
   * Program threw, not where any one child call happened; attributing it to
   * a child's selection would be a fabricated location, so this must be
   * false for every record but the Run's own.
   */
  locatable: boolean
  /** The selected record, whose stored location applies when the Run itself is not selected. */
  selectedRow: ActivityRow | null
  /**
   * The selected Tool Invocation or Program Run Event's position among the Run's records,
   * when known. Between two known positions, the detail slides in that order.
   */
  position?: number
  /** The drawer's close control. The selected record's own sticky header holds it. */
  close?: React.ReactNode
  children: React.ReactNode
}) {
  // The detail pane's share of the split, 0 to 1; the source pane takes the rest.
  const [splitRatio, setSplitRatio] = useState(0.5)
  // The record's decision controls render here, below its scrolling evidence.
  const [footer, setFooter] = useState<HTMLElement | null>(null)
  const [panelLabel, setPanelLabel] = useState<string>()
  const splitRef = useRef<HTMLDivElement>(null)
  // Each record's detail opens at its top; the source pane keeps its own position.
  // A layout effect here runs after the scroll area attaches its viewport ref again.
  const detailRef = useRef<HTMLDivElement>(null)
  const record = selectedRow?.name
  const opened = useRef(record)
  useLayoutEffect(() => {
    if (opened.current === record) return
    opened.current = record
    detailRef.current?.scrollTo?.({ top: 0 })
  })
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
  const missingLocationMessage =
    !location && selectedRow && madeByCall(selectedRow) ? 'Source location unavailable' : undefined

  return (
    <div className={styles.root}>
      <div className={styles.split} ref={splitRef}>
        <section
          aria-label="Selected record"
          className={styles.detailPane}
          style={{ flexGrow: splitRatio, flexBasis: 0 }}
        >
          {/* The record's evidence and its decision footer slide together as one card. */}
          <DetailMotion record={record} position={position}>
            {/* A labelled Panel, such as a review, names this one region around both its
                evidence and its decision; without a label it is no region of its own. */}
            <section aria-label={panelLabel} className={styles.detailRegion}>
              {/* The record's header, with its close control, wraps to the pane's width. */}
              <ScrollArea
                className={styles.detailScroll}
                viewportRef={detailRef}
                surface="floating"
                constrainWidth
                scrollDirection="vertical"
              >
                <div key={record} className={styles.detailContent}>
                  {/* The record's own header names it and holds the close control; the split
                      adds no Run header of its own, so a Run's header shows once. */}
                  <PanelInset value>
                    <PanelInsetLabel value={setPanelLabel}>
                      <PanelFooterSlot value={footer}>
                        <PanelClose value={close}>{children}</PanelClose>
                      </PanelFooterSlot>
                    </PanelInsetLabel>
                  </PanelInset>
                </div>
              </ScrollArea>
              {/* Outside the scroll area, the decision never scrolls away or sits under its fade. */}
              <Card.Footer ref={setFooter} className={styles.detailFooter} />
            </section>
          </DetailMotion>
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
            // Each record has its own location, so selecting another one reveals it again.
            revealKey={`${context.run.name}:${selectedRow?.name ?? ''}`}
          />
        </section>
      </div>
    </div>
  )
}
