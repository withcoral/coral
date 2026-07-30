import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import { DesktopUpdateIndicator } from './desktop-update-indicator'
import type { DesktopUpdateIndicatorState } from './desktop-update-indicator'

describe('DesktopUpdateIndicator', () => {
  it.each([
    [
      { status: 'available', version: '0.9.0' },
      'Update available',
      'Coral 0.9.0',
      'Coral 0.9.0 is available and will download automatically.',
    ],
    [
      { status: 'downloading', version: '0.9.0' },
      'Downloading',
      'Coral 0.9.0',
      'Coral 0.9.0 is downloading.',
    ],
    [
      { status: 'ready', version: '0.9.0' },
      'Update ready',
      'Restart to install',
      'Coral 0.9.0 is ready. Restart to install.',
    ],
  ] as const)('presents the %s state', async (state, title, detail, accessibleLabel) => {
    const screen = await render(
      <DesktopUpdateIndicator
        isMinimized={false}
        state={state satisfies DesktopUpdateIndicatorState}
      />,
    )

    await expect.element(screen.getByText(title)).toBeVisible()
    await expect.element(screen.getByText(detail)).toBeVisible()
    await expect
      .element(screen.getByRole('status', { name: accessibleLabel }))
      .not.toHaveAttribute('tabindex')
  })

  it('keeps the minimized indicator keyboard accessible and explains it in a tooltip', async () => {
    const accessibleLabel = 'Coral 0.9.0 is ready. Restart to install.'
    const screen = await render(
      <DesktopUpdateIndicator isMinimized state={{ status: 'ready', version: '0.9.0' }} />,
    )
    const indicator = screen.getByRole('status', { name: accessibleLabel })

    await expect.element(indicator).toHaveAttribute('tabindex', '0')
    await expect.element(screen.getByText('Update ready')).not.toBeInTheDocument()
    await expect.element(screen.getByText('Restart to install')).not.toBeInTheDocument()
    await indicator.hover()
    await expect
      .poll(() => document.querySelector('[data-base-ui-portal]')?.textContent)
      .toContain(accessibleLabel)
  })
})
