import { Dialog as BaseDialog } from '@base-ui/react/dialog'
import { useContext } from 'react'

import { RailContext } from './rail'

/** Moves the popup to `<body>`, or into its docked rail's place in the page. */
export function Portal({ container, ...props }: BaseDialog.Portal.Props) {
  const rail = useContext(RailContext)
  return <BaseDialog.Portal container={container ?? rail?.dock} {...props} />
}
