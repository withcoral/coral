import { Dialog as BaseDialog } from '@base-ui/react/dialog'

export { Actions } from './actions'
export type { ActionsProps } from './actions'
export { Backdrop } from './backdrop'
export type { BackdropProps } from './backdrop'
export { Close } from './close'
export type { CloseProps } from './close'
export { Description } from './description'
export type { DescriptionProps } from './description'
export { Expand } from './expand'
export { Popup } from './popup'
export { Portal } from './portal'
export type { PopupProps, PopupSize } from './popup'
export { Rail } from './rail'
export type { RailMode, RailProps } from './rail'
export { Header } from './header'
export { Title } from './title'
export type { TitleProps } from './title'
export { Trigger } from './trigger'
export { useCloseAfterExit } from './use-close-after-exit'
export type { TriggerProps } from './trigger'

export const Root = BaseDialog.Root

/** Pins content to the popup's top edge, flush like `Header`. */
export { pinned as pinnedClassName } from './dialog.css'
/** Pins content to the popup's bottom edge. */
export { pinnedBottom as pinnedBottomClassName } from './dialog.css'
/** Reaches through the popup's side padding to its edges. */
export { flushInline as flushInlineClassName } from './dialog.css'
