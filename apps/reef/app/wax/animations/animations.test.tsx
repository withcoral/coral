import { afterEach, describe, expect, it } from 'vitest'
import { cdp } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import { animations } from '@/wax/animations'

async function emulateReducedMotion(value?: 'no-preference' | 'reduce') {
  await cdp().send('Emulation.setEmulatedMedia', {
    features: value ? [{ name: 'prefers-reduced-motion', value }] : [],
  })
}

afterEach(async () => {
  await emulateReducedMotion()
})

describe('Wax animations', () => {
  it('stops shared animations when reduced motion is requested', async () => {
    await emulateReducedMotion('no-preference')

    const screen = await render(
      <>
        <div className={animations.pulseAnimation} data-testid="pulse" />
        <div className={animations.spinAnimation} data-testid="spin" />
      </>,
    )
    const pulse = screen.getByTestId('pulse').element()
    const spin = screen.getByTestId('spin').element()

    expect(getComputedStyle(pulse).animationName).not.toBe('none')
    expect(getComputedStyle(spin).animationName).not.toBe('none')

    await emulateReducedMotion('reduce')

    expect(getComputedStyle(pulse).animationName).toBe('none')
    expect(getComputedStyle(spin).animationName).toBe('none')
  })
})
