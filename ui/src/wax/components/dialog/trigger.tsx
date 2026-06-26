import { Dialog as BaseDialog } from '@base-ui-components/react/dialog'

export type TriggerProps = React.ComponentProps<typeof BaseDialog.Trigger>

export function Trigger(props: TriggerProps) {
  return <BaseDialog.Trigger {...props} />
}
