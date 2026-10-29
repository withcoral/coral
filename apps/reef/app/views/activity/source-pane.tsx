import { AnnotatedCodeBlock } from '@/wax/components/code-block/annotated-code-block'
import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'
import type { SourceDocument, SourceHighlight } from './source-location'
import * as styles from './source-pane.css'

/**
 * The persistent, read-only accepted-source pane: one document over Wax's
 * `AnnotatedCodeBlock` for line numbers, syntax highlighting, the highlighted
 * range, and its reveal whenever `revealKey` changes. A Saved Function
 * snapshot is named; the accepted program is the pane's default and is not.
 * An explicit "no recorded location" message replaces a guessed one when the
 * selected record has a call but no mapped range.
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
  return (
    <div className={styles.root}>
      {(document.label || highlight) && (
        <div className={styles.header}>
          {document.label && (
            <Typography.BodySmallStrong>{document.label}</Typography.BodySmallStrong>
          )}
          {/* The highlighted line in words, so it is not told by color alone. */}
          {highlight && (
            <span className={styles.location} role="status">
              <Icon name="MapPin" size="14" color="inherit" />
              {highlight.startLine === highlight.endLine
                ? `Line ${highlight.startLine}`
                : `Lines ${highlight.startLine}–${highlight.endLine}`}
            </span>
          )}
        </div>
      )}
      {missingLocationMessage && (
        <div className={styles.missing} role="status">
          <Icon name="MapPinOff" size="14" color="inherit" />
          <Typography.BodySmall>{missingLocationMessage}</Typography.BodySmall>
        </div>
      )}
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
