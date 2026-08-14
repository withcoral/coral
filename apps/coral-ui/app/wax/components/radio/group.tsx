import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group'
import classNames from 'classnames'

import { Container as ScrollArea } from '@/wax/components/scroll-area'

import * as styles from './radio.css'

export type GroupProps<Value> = Omit<BaseRadioGroup.Props<Value>, 'className' | 'onValueChange'> & {
  className?: string
  onValueChange?: (value: Value) => void
}

export function Group<Value>({
  children,
  className,
  onValueChange,
  ref,
  ...props
}: GroupProps<Value>) {
  return (
    <ScrollArea fade="horizontal" height="auto" scrollDirection="horizontal">
      <BaseRadioGroup
        className={classNames(styles.group, className)}
        onValueChange={(value) => onValueChange?.(value)}
        ref={ref}
        {...props}
      >
        {children}
      </BaseRadioGroup>
    </ScrollArea>
  )
}
