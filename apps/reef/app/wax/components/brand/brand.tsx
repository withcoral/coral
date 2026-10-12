import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import * as styles from './brand.css'

interface BrandProps {
  compact?: boolean
}

/** Coral's shared lockup; compact navigation shows only the mark. */
export function Brand({ compact = false }: BrandProps) {
  return (
    <span className={styles.brand} role="img" aria-label="Coral">
      <Icon name="Coral" size="30" />
      {!compact && (
        <Typography.HeadingLarge as="span" className={styles.wordmark}>
          Coral
        </Typography.HeadingLarge>
      )}
    </span>
  )
}
