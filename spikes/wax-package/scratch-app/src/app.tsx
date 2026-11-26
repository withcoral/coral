import { Banner, Button, Typography } from '@withcoral/wax/components'
import { Icon } from '@withcoral/wax/components/icon'
import { Pill } from '@withcoral/wax/components/pill'
import { useTheme } from '@withcoral/wax/theme/theme-provider'

import * as styles from './app.css'

export function App() {
  const { themeClass } = useTheme()
  return (
    <div className={`${themeClass} ${styles.page}`} data-testid="page">
      <Typography.Body>Wax from a package</Typography.Body>
      <Banner variant="info">A banner</Banner>
      <Button.TextButton icon="ShieldCheck">Save</Button.TextButton>
      <Icon name="ShieldCheck" />
      <Pill>pill</Pill>
    </div>
  )
}
