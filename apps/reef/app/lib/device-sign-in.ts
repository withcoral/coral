/**
 * Form attributes for the button that leaves Coral to sign in. A device
 * sign-in never returns to Coral, so it opens in a new tab and the page stays
 * to pick up the result. Desktop runs that tab hidden and hands its consent
 * redirect to the system browser.
 */
export function signInFormTarget(device: boolean): { target?: '_blank'; rel?: 'noopener' } {
  return device ? { target: '_blank', rel: 'noopener' } : {}
}
