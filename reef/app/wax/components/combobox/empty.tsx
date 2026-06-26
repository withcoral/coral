import { Combobox as BaseCombobox } from '@base-ui/react/combobox'

import * as styles from './combobox.css'

export interface EmptyProps {
  children?: React.ReactNode
}

export function Empty({ children = 'No results found' }: EmptyProps) {
  return <BaseCombobox.Empty className={styles.empty}>{children}</BaseCombobox.Empty>
}
