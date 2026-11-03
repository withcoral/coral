import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'
import { useContext } from 'react'

import { useComposedRefs } from '@/utils/compose-refs'

import * as styles from './dialog.css'
import { ForceBackdropContext, RailContext } from './rail'

export interface PopupProps extends BaseDialog.Popup.Props {
  children: React.ReactNode
  ref?: React.Ref<HTMLDivElement>
  /** The width of the dialog. Defaults to 'm'. */
  size?: PopupSize
  placement?: 'center' | 'left' | 'right'
  /** Long-form review uses the full narrow viewport rather than leaving a backdrop gutter. */
  fullWidthOnMobile?: boolean
}

export type PopupSize = 'l' | 'm' | 'xl' | 'xxl'

export function Popup({
  children,
  className,
  ref,
  size = 'm',
  placement = 'center',
  fullWidthOnMobile = false,
  initialFocus,
  ...props
}: PopupProps) {
  const rail = useContext(RailContext)
  const besidePage = rail !== null && rail.mode !== 'modal'
  const composedRef = useComposedRefs(ref, rail?.popup)
  return (
    <BaseDialog.Popup
      className={classNames(
        styles.popup,
        styles.popupSize[size],
        placement !== 'center' && styles.drawer,
        placement === 'right' && styles.rightDrawer,
        fullWidthOnMobile && styles.fullWidthOnMobile,
        besidePage && styles.besidePage,
        rail?.mode === 'docked' && styles.dockedPopup,
        className,
      )}
      ref={composedRef}
      // A modal rail always takes focus; beside the page, the caller may keep it where it is.
      initialFocus={rail?.mode === 'modal' ? undefined : initialFocus}
      {...props}
    >
      {/* The mode is this rail's own: a dialog opened from its content is a plain dialog. */}
      <RailContext value={null}>
        <ForceBackdropContext value={besidePage}>{children}</ForceBackdropContext>
      </RailContext>
    </BaseDialog.Popup>
  )
}
