import { CoralMark, CoralWordmark } from './brand-art'
import * as styles from './brand.css'

interface BrandProps {
  compact?: boolean
}

/** Coral's shared lockup; compact navigation shows only the mark. */
export function Brand({ compact = false }: BrandProps) {
  return (
    <span className={styles.brand} role="img" aria-label="Coral">
      <CoralMark className={styles.mark} />
      {!compact && <CoralWordmark className={styles.wordmark} />}
    </span>
  )
}
