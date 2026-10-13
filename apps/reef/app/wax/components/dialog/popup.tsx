import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'
import { useCallback, useContext } from 'react'

import { BesidePageContext, OwnBackdropContext } from './beside-page-root'

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
  // React 19 runs a returned cleanup instead of passing null, so the caller's
  // own cleanup runs here, or its ref is cleared as React would.
  const registered = useCallback(
    (element: HTMLDivElement) => {
      register?.(element)
      const cleanup = typeof ref === 'function' ? ref(element) : undefined
      if (ref && typeof ref !== 'function') ref.current = element
      return () => {
        register?.(null)
        if (typeof cleanup === 'function') cleanup()
        else if (typeof ref === 'function') ref(null)
        else if (ref) ref.current = null
      }
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
      {/* The mode belongs to this drawer: a dialog opened from its content is its own. */}
      {besidePage ? (
        <BesidePageContext value={null}>
          <OwnBackdropContext value={besidePage.beside}>{children}</OwnBackdropContext>
        </BesidePageContext>
      ) : (
        children
      )}
    </BaseDialog.Popup>
  )
}
