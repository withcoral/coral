import { Tabs as BaseTabs } from '@base-ui/react/tabs'
import classNames from 'classnames'

import { Button } from '@/wax/components'

import * as styles from './tabs.css'

export function Tab({
  children,
  className,
  disabled,
  ...props
}: React.ComponentPropsWithoutRef<typeof BaseTabs.Tab>) {
  return (
    <BaseTabs.Tab
      render={
        <Button.TextButton
          className={classNames(styles.tab, className)}
          disabled={disabled}
          variant="bare"
        >
          {children}
        </Button.TextButton>
      }
      {...props}
    />
  )
}
