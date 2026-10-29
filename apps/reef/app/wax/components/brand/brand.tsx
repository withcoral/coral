import { CoralMark, CoralWordmark } from './brand-art'
import * as styles from './brand.css'

interface BrandProps {
  compact?: boolean
  /** Names the product beside the wordmark, such as "console". It sits on the wordmark's baseline. */
  label?: string
}

/** Coral's shared lockup; compact navigation shows only the mark. */
export function Brand({ compact = false, label }: BrandProps) {
  return (
    <span className={styles.brand} role="img" aria-label={label ? `Coral ${label}` : 'Coral'}>
      <CoralMark className={styles.mark} />
      {!compact && (
        // The wordmark has no text baseline, so a row aligns on its bottom
        // edge, which is where its letters sit. The label's baseline meets it.
        <span className={styles.wordmarkRow}>
          <CoralWordmark className={styles.wordmark} />
          {label && <span className={styles.label}>{label}</span>}
        </span>
      )}
    </span>
  )
}
