import { Tabs as BaseTabs } from '@base-ui/react/tabs'
import classNames from 'classnames'

import * as styles from './tabs.css'

export function Indicator({
  className,
  ...props
}: React.ComponentPropsWithoutRef<typeof BaseTabs.Indicator>) {
  return <BaseTabs.Indicator className={classNames(styles.indicator, className)} {...props} />
}
