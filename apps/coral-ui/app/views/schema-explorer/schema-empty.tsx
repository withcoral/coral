import { Typography } from '@/wax/components/typography'

import * as styles from './schema-explorer.css'

// Rendered at /schema (index) before a catalog item is selected.
export function TableDetailEmpty() {
  return (
    <div className={styles.detailEmpty}>
      <Typography.Body variant="secondary">
        Select a table or table function from the schema tree to inspect it.
      </Typography.Body>
    </div>
  )
}
