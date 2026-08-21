import { Combobox as BaseCombobox } from '@base-ui/react/combobox'
import classNames from 'classnames'
import { RefObject } from 'react'

import * as styles from './combobox.css'

export interface InputProps {
  ariaLabelledby?: string
  bare?: boolean
  className?: string
  placeholder?: string
  ref?: RefObject<HTMLInputElement | null>
}

export function Input({ ariaLabelledby, bare = false, className, placeholder, ref }: InputProps) {
  return (
    <BaseCombobox.Input
      aria-labelledby={ariaLabelledby}
      className={classNames(bare ? styles.inputBare : styles.input, className)}
      placeholder={placeholder}
      ref={ref}
    />
  )
}
