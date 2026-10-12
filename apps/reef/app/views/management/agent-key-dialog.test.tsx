/** @vitest-environment happy-dom */
import { act } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, expect, it, vi } from 'vitest'
import { AgentKeyDialog } from './agent-key-dialog'

let root: Root | undefined
afterEach(() => {
  act(() => root?.unmount())
  document.body.innerHTML = ''
})

it('masks the new key, focuses Copy API key and close, and ignores Escape', async () => {
  const writeText = vi.fn(() => Promise.resolve())
  Object.defineProperty(navigator, 'clipboard', {
    value: { writeText },
    configurable: true,
  })
  const onClose = vi.fn()
  root = createRoot(document.body.appendChild(document.createElement('div')))
  act(() =>
    root!.render(
      <AgentKeyDialog credential="coral_secret_key" agentName="Triage" onClose={onClose} />,
    ),
  )
  const dialog = document.querySelector('[role="dialog"]')!
  expect(dialog.textContent).not.toContain('coral_secret_key')
  const copy = dialog.querySelector<HTMLButtonElement>(
    'button[aria-label="Copy API key and close"]',
  )!
  await act(() => new Promise((resolve) => setTimeout(resolve, 50)))
  expect(document.activeElement).toBe(copy)
  act(() => document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })))
  expect(document.querySelector('[role="dialog"]')).toBe(dialog)
  expect(onClose).not.toHaveBeenCalled()
  await act(async () => copy.click())
  expect(writeText).toHaveBeenCalledWith('coral_secret_key')
  expect(onClose).toHaveBeenCalledOnce()
})

it('closes without the clipboard, after a failed copy', async () => {
  const writeText = vi.fn(() => Promise.reject(new Error('not allowed')))
  Object.defineProperty(navigator, 'clipboard', {
    value: { writeText },
    configurable: true,
  })
  const onClose = vi.fn()
  root = createRoot(document.body.appendChild(document.createElement('div')))
  act(() => root!.render(<AgentKeyDialog credential="coral_secret_key" onClose={onClose} />))
  const dialog = document.querySelector('[role="dialog"]')!
  await act(async () =>
    dialog.querySelector<HTMLButtonElement>('button[aria-label="Copy API key and close"]')!.click(),
  )
  expect(onClose).not.toHaveBeenCalled()
  const close = [...dialog.querySelectorAll('button')].find((b) => b.textContent === 'Close')!
  act(() => close.click())
  expect(onClose).toHaveBeenCalledOnce()
})

// Catches: a client's setup shown with the key in clear text, or copied
// without it, or copying it closing the dialog before the key is saved.
const ENDPOINT = 'https://coral.example.com/mcp'
it.each([
  [
    'Claude Code',
    'Copy Claude Code command',
    `claude mcp add --transport http coral ${ENDPOINT} --header "Authorization: Bearer coral_secret_key"`,
  ],
  [
    'Codex',
    'Copy Codex configuration',
    `[mcp_servers.coral]\nurl = "${ENDPOINT}"\nbearer_token_env_var = "CORAL_API_KEY"`,
  ],
  ['Other', 'Copy MCP URL', ENDPOINT],
  ['Other', 'Copy Authorization header', 'Bearer coral_secret_key'],
])('copies the %s setup with the key, but shows it masked', async (client, name, value) => {
  const writeText = vi.fn(() => Promise.resolve())
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
  const onClose = vi.fn()
  root = createRoot(document.body.appendChild(document.createElement('div')))
  act(() =>
    root!.render(
      <AgentKeyDialog credential="coral_secret_key" endpoint={ENDPOINT} onClose={onClose} />,
    ),
  )
  const dialog = document.querySelector('[role="dialog"]')!
  const tab = [...dialog.querySelectorAll<HTMLElement>('[role="tab"]')].find(
    (el) => el.textContent === client,
  )!
  await act(async () => tab.click())
  expect(dialog.innerHTML).not.toContain('coral_secret_key')
  await act(async () => dialog.querySelector<HTMLButtonElement>(`[aria-label="${name}"]`)!.click())
  expect(writeText).toHaveBeenCalledWith(value)
  expect(onClose).not.toHaveBeenCalled()
})

it('shows only the key without an MCP endpoint', () => {
  root = createRoot(document.body.appendChild(document.createElement('div')))
  act(() => root!.render(<AgentKeyDialog credential="coral_secret_key" onClose={() => {}} />))
  expect(document.querySelector('[role="tab"]')).toBeNull()
})
