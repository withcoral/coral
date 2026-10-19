import { Dialog as BaseDialog } from '@base-ui/react/dialog'

/**
 * Passes every other prop and the ref to Base UI's trigger, so a wrapper such
 * as a tooltip reaches the button with its handlers.
 */
export type TriggerProps = BaseDialog.Trigger.Props & {
  children: React.ReactNode
  ref?: React.Ref<HTMLButtonElement>
}

export function Trigger(props: TriggerProps) {
  return <BaseDialog.Trigger {...props} />
}
