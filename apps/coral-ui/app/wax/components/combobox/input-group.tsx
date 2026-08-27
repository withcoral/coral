import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'
import { useCallback } from 'react'

import { useAnchorContext } from './anchor-context'
import * as styles from './combobox.css'

export interface InputGroupProps {
  children: React.ReactNode
  className?: string
}

export function InputGroup({ children, className }: InputGroupProps) {
  const anchorContext = useAnchorContext()
  const setInputGroupAnchor = useCallback(
    (element: HTMLDivElement | null) => {
      anchorContext?.setInputGroupAnchor(element)
    },
    [anchorContext],
  )

  return (
    <BaseCombobox.InputGroup
      className={classNames(styles.inputGroup, className)}
      ref={setInputGroupAnchor}
    >
      {children}
    </BaseCombobox.InputGroup>
  )
}
