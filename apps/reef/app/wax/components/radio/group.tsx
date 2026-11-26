import { RadioGroup as BaseRadioGroup } from '@base-ui/react/radio-group'
import classNames from 'classnames'

import { Container as ScrollArea } from '@/wax/components/scroll-area'

import * as styles from './radio.css'

export type GroupProps<Value> = Omit<BaseRadioGroup.Props<Value>, 'className' | 'onValueChange'> & {
  className?: string
  /** Grid places cards two per row, one on narrow screens. */
  layout?: 'row' | 'stack' | 'grid'
  onValueChange?: (value: Value) => void
}

export function Group<Value>({
  children,
  className,
  layout = 'row',
  onValueChange,
  ref,
  ...props
}: GroupProps<Value>) {
  const group = (
    <BaseRadioGroup
      className={classNames(
        layout === 'stack' ? styles.stack : layout === 'grid' ? styles.grid : styles.group,
        className,
      )}
      aria-orientation={layout === 'stack' ? 'vertical' : 'horizontal'}
      onValueChange={(value) => onValueChange?.(value)}
      ref={ref}
      {...props}
    >
      {children}
    </BaseRadioGroup>
  )
  return layout !== 'row' ? (
    group
  ) : (
    <ScrollArea fade="horizontal" height="auto" scrollDirection="horizontal">
      {group}
    </ScrollArea>
  )
}
