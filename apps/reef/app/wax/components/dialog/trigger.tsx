import { Dialog as BaseDialog } from '@base-ui/react/dialog'

export interface TriggerProps {
  children: React.ReactNode
  className?: string
  render?: React.ReactElement<Record<string, unknown>>
}

export function Trigger({ children, className, render }: TriggerProps) {
  return (
    <BaseDialog.Trigger className={className} render={render}>
      {children}
    </BaseDialog.Trigger>
  )
}
