import { AnnotatedCodeBlock } from '@/wax/components/code-block/annotated-code-block'
import { Icon } from '@/wax/components/icon'
import { Switch } from '@/wax/components/inputs'
import { Typography } from '@/wax/components/typography'
import type { SourceDocument, SourceHighlight } from './source-location'
import * as styles from './source-pane.css'

/**
 * The persistent, read-only accepted-source pane: one document, labeled and
 * marked read-only, over Wax's `AnnotatedCodeBlock` for line numbers, syntax
 * highlighting, the highlighted range, and the follow-selection reveal. An
 * explicit "no recorded location" message replaces a guessed one when there
 * is no mapped range.
 */
export function SourcePane({
  document,
  highlight,
  missingLocationMessage,
  followSelection,
  onFollowSelectionChange,
  revealKey,
}: {
  document: SourceDocument
  highlight: SourceHighlight | null
  missingLocationMessage?: string
  followSelection: boolean
  onFollowSelectionChange: (follow: boolean) => void
  revealKey: string
}) {
  return (
    <div className={styles.root}>
      <div className={styles.header}>
        <Typography.BodySmallStrong>{document.label}</Typography.BodySmallStrong>
        <span className={styles.readOnly}>
          <Icon name="Lock" size="14" color="inherit" />
          Read-only snapshot
        </span>
        {/* The highlighted line in words, so it is not told by color alone. */}
        {highlight && (
          <span className={styles.location} role="status">
            <Icon name="MapPin" size="14" color="inherit" />
            {highlight.startLine === highlight.endLine
              ? `Line ${highlight.startLine}`
              : `Lines ${highlight.startLine}–${highlight.endLine}`}
          </span>
        )}
        <div className={styles.follow}>
          <Switch
            checked={followSelection}
            onCheckedChange={onFollowSelectionChange}
            aria-label="Follow selection"
          />
          <Typography.BodySmall>Follow selection</Typography.BodySmall>
        </div>
      </div>
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
        followHighlight={followSelection}
        revealKey={revealKey}
        surface="floating"
      />
    </div>
  )
}
