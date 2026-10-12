import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import classNames from 'classnames'

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
  return (
    <BaseDialog.Popup
      className={classNames(
        styles.popup,
        styles.popupSize[size],
        placement !== 'center' && styles.drawer,
        placement === 'right' && styles.rightDrawer,
        fullWidthOnMobile && styles.fullWidthOnMobile,
        className,
      )}
      ref={ref}
      {...props}
    >
      {children}
    </BaseDialog.Popup>
  )
}
