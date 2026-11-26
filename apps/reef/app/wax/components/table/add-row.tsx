import type { ReactNode } from 'react'

import { Icon } from '@/wax/components/icon'

import { Cell } from './cell'
import { Row } from './row'
import * as styles from './table.css'

/** A last row that adds a row. The whole row is its button. */
export function AddRow({ children, onClick }: { children: ReactNode; onClick: () => void }) {
  return (
    <Row>
      <Cell fullWidth className={styles.addRowCell}>
        <button type="button" className={styles.addRow} onClick={onClick}>
          <Icon name="Plus" size="16" />
          {children}
        </button>
      </Cell>
    </Row>
  )
}
