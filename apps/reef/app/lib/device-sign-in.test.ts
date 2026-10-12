import { expect, it } from 'vitest'
import { signInFormTarget } from './device-sign-in'

it.each([
  { device: true, newTab: true },
  { device: false, newTab: false },
])('opens a new tab for device=$device: $newTab', ({ device, newTab }) => {
  expect(signInFormTarget(device)).toEqual(newTab ? { target: '_blank', rel: 'noopener' } : {})
})
