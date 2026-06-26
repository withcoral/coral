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
    <div className={classNames(styles.inputGroup, className)} ref={setInputGroupAnchor}>
      {children}
    </div>
  )
}
