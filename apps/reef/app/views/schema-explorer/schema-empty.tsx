import { Typography } from '@/wax/components/typography'

import * as styles from './schema-explorer.css'

// Rendered at /schema (index) before a table is selected.
export function TableDetailEmpty() {
  return (
    <div className={styles.detailEmpty}>
      <Typography.Body variant="secondary">
        Select a table from the schema tree to inspect its columns.
      </Typography.Body>
    </div>
  )
}
