import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'
import { useCallback, useContext } from 'react'

import { BesidePageContext } from './beside-page-root'

import * as styles from './dialog.css'

export interface PopupProps extends BaseDialog.Popup.Props {
  children: React.ReactNode
  ref?: React.Ref<HTMLDivElement>
  /** The width of the dialog. Defaults to 'm'. */
  size?: PopupSize
  placement?: 'center' | 'left' | 'right'
  /** Long-form review uses the full narrow viewport rather than leaving a backdrop gutter. */
  fullWidthOnMobile?: boolean
}

export type PopupSize = 'l' | 'm' | 'xl'

export function Popup({
  children,
  className,
  ref,
  size = 'm',
  placement = 'center',
  fullWidthOnMobile = false,
  ...props
}: PopupProps) {
  const besidePage = useContext(BesidePageContext)
  const register = besidePage?.register
  // A root beside the page focuses the popup when it becomes modal.
  const registered = useCallback(
    (element: HTMLDivElement | null) => {
      register?.(element)
      if (typeof ref === 'function') ref(element)
      else if (ref) ref.current = element
    },
    [register, ref],
  )
  return (
    <BaseDialog.Popup
      className={classNames(
        styles.popup,
        styles.popupSize[size],
        placement !== 'center' && styles.drawer,
        placement === 'right' && styles.rightDrawer,
        fullWidthOnMobile && styles.fullWidthOnMobile,
        besidePage?.beside && styles.besidePage,
        className,
      )}
      ref={register ? registered : ref}
      {...props}
    >
      {children}
    </BaseDialog.Popup>
  )
}
