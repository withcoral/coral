import { useMemo, useState } from 'react'
import { downloadText } from '@/utils/download'
import { payloadQuery } from '@/utils/filter-query'
import * as Button from '@/wax/components/button'
import { CopyButton } from '@/wax/components/button/copy-button'
import { AnnotatedCodeBlock } from '@/wax/components/code-block/annotated-code-block'
import * as Dialog from '@/wax/components/dialog'
import { FindControls } from '@/wax/components/find-controls'
import type { Highlight } from '@/wax/components/highlighted-matches'
import { Icon } from '@/wax/components/icon'
import { findMatches, lineUnits } from '@/wax/components/payload-viewer/payload-search'
import { Typography } from '@/wax/components/typography'
import { useActivityInspect } from './live'
import type { SourceDocument, SourceHighlight } from './source-location'
import * as styles from './source-pane.css'

/**
 * The persistent, read-only accepted-source pane: one document over Wax's
 * `AnnotatedCodeBlock` for line numbers, syntax highlighting, the highlighted
 * range, and its reveal whenever `revealKey` changes. A Saved Function
 * snapshot is named; the accepted program is the pane's default and is not.
 * An explicit "no recorded location" message replaces a guessed one when the
 * selected record has a call but no mapped range.
 *
 * Copy, Download and Expand sit at the header's right, as on a payload. Copy and
 * Download take the document exactly as Coral retained it. Expand opens it in a
 * tall dialog at the same range, with Find over every line, and live mode holds the
 * record meanwhile.
 */
export function SourcePane({
  document,
  highlight,
  missingLocationMessage,
  revealKey,
}: {
  document: SourceDocument
  highlight: SourceHighlight | null
  missingLocationMessage?: string
  revealKey: string
}) {
  const title = document.label ?? 'Accepted source'
  const inspect = useActivityInspect()
  const actions = (expand?: React.ReactNode) => (
    <div className={styles.actions} role="group" aria-label="Source actions">
      <CopyButton
        size="22"
        variant="bare"
        ariaLabel={`Copy ${title}`}
        tooltipText="Copy"
        textToCopy={document.code}
      />
      <Button.IconButton
        name="Download"
        size="22"
        variant="bare"
        ariaLabel={`Download ${title}`}
        tooltipText="Download"
        onClick={() => downloadText(document.code, document.fileName, 'text/plain')}
      />
      {expand}
    </div>
  )
  // The highlighted line in words, so it is not told by color alone.
  const location = highlight && (
    <span className={styles.location} role="status">
      <Icon name="MapPin" size="14" color="inherit" />
      {highlight.startLine === highlight.endLine
        ? `Line ${highlight.startLine}`
        : `Lines ${highlight.startLine}–${highlight.endLine}`}
    </span>
  )
  const missing = missingLocationMessage && (
    <div className={styles.missing} role="status">
      <Icon name="MapPinOff" size="14" color="inherit" />
      <Typography.BodySmall>{missingLocationMessage}</Typography.BodySmall>
    </div>
  )
  return (
    <div className={styles.root}>
      <div className={styles.header}>
        {document.label && (
          <Typography.BodySmallStrong>{document.label}</Typography.BodySmallStrong>
        )}
        {location}
        {actions(
          <Dialog.Expand label={title} onExpandedChange={inspect}>
            <ExpandedSource
              label={title}
              code={document.code}
              highlight={highlight}
              revealKey={revealKey}
              context={
                <>
                  <Typography.BodySmall variant="tertiary">TypeScript</Typography.BodySmall>
                  {location}
                </>
              }
              actions={actions()}
              missing={missing}
            />
          </Dialog.Expand>,
        )}
      </div>
      {missing}
      <AnnotatedCodeBlock
        code={document.code}
        language="typescript"
        highlight={highlight}
        followHighlight
        revealKey={revealKey}
      />
    </div>
  )
}

/** The source expanded: its language and location, Find, its actions, then the code. */
function ExpandedSource({
  label,
  code,
  highlight,
  revealKey,
  context,
  actions,
  missing,
}: {
  label: string
  code: string
  highlight: SourceHighlight | null
  revealKey: string
  context: React.ReactNode
  actions: React.ReactNode
  missing: React.ReactNode
}) {
  const [query, setQuery] = useState('')
  const [current, setCurrent] = useState(0)
  const lines = useMemo(() => lineUnits(code), [code])
  const { matches } = useMemo(() => findMatches(lines, payloadQuery(query)), [lines, query])
  // A match past the end, as after the source changes, gives way to the last one.
  const at = Math.min(current, matches.length - 1)
  /** By line number, the parts of its text each match found. */
  const marks = useMemo(() => {
    const byLine = new Map<number, Highlight[]>()
    matches.forEach(({ ranges, units: [line] }, index) => {
      const { start } = lines[line!]!
      const list = byLine.get(line! + 1) ?? []
      for (const [from, to] of ranges)
        list.push({ range: [from - start, to - start], current: index === at })
      byLine.set(line! + 1, list)
    })
    return byLine
  }, [matches, lines, at])
  const status = !query.trim()
    ? ''
    : matches.length
      ? `${at + 1} of ${matches.length}`
      : 'No matches'
  return (
    <>
      <div className={styles.header}>
        {context}
        <FindControls
          label={label}
          query={query}
          onQueryChange={(next) => {
            setQuery(next)
            setCurrent(0)
          }}
          status={status}
          count={matches.length}
          onStep={(by) =>
            setCurrent(matches.length ? (at + by + matches.length) % matches.length : 0)
          }
        />
        {actions}
      </div>
      {missing}
      <div className={styles.expandedCode}>
        <AnnotatedCodeBlock
          code={code}
          language="typescript"
          highlight={highlight}
          followHighlight
          revealKey={revealKey}
          marks={marks}
        />
      </div>
    </>
  )
}
