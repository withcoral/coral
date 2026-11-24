/** @vitest-environment happy-dom */

import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ComponentProps } from 'react'
import { expect, it } from 'vitest'
import { renderLivePage, renderRoute } from '@/test-support/route'
import { SubAgentPage } from './sub-agent-page'
import type { RefusedSave, SubAgentModelView } from './types'

type Page = ComponentProps<typeof SubAgentPage>

const form = { csrf: 'test-csrf', requestId: 'test-request' }
const model: SubAgentModelView = {
  values: {
    protocol: 'anthropic',
    baseUrl: 'https://api.example.com/v1',
    model: 'large-1',
    contextWindow: '200000',
    requestArgs: '{\n  "max_tokens": 4096\n}',
  },
  hasApiKey: true,
  changedBy: { user: 'users/ada', name: 'Ada Lovelace', email: 'ada@example.com', local: false },
  changedAt: '2026-11-20T14:03:00Z',
  etag: 'etag-1',
}
const render = (page: Partial<Page> = {}) =>
  renderRoute(<SubAgentPage model={model} form={form} {...page} />)

const baseUrl = () => screen.getByLabelText(/^Base URL/)
/** The input that holds a key to save, or null while the form asks for none. */
const keyInput = () => screen.queryByLabelText(/^Key to save/)
const keyChoices = () =>
  within(screen.getByRole('radiogroup', { name: 'API key' }))
    .getAllByRole('radio')
    .map((radio) => `${radio.getAttribute('aria-checked') === 'true' ? '*' : ''}${nameOf(radio)}`)
const nameOf = (radio: HTMLElement) => radio.closest('label')!.textContent
/** What Save posts. */
const posted = () => {
  const save = screen.getByRole<HTMLButtonElement>('button', { name: 'Save' })
  return Object.fromEntries(new FormData(save.form!))
}
const retype = async (input: HTMLElement, text: string) => {
  await userEvent.clear(input)
  await userEvent.type(input, text)
}

const turnsOff = 'Saving a new base URL turns the sub-agent off in every workspace.'

// Catches: a new base URL that drops the saved key, or turns the sub-agent off, without a word,
// and a slash at the end that asks for the key though Coral keeps it.
it.each([
  ['https://api.example.com/v1', false],
  ['https://api.example.com/v1/', false],
  [' https://api.example.com/v1// ', false],
  ['https://api.example.com/v2', true],
  ['http://api.example.com/v1', true],
])('for the base URL "%s", asks for the key again: %s', async (url, again) => {
  render()
  await retype(baseUrl(), url)
  expect(screen.queryByText(turnsOff) !== null).toBe(again)
  expect(keyInput() !== null).toBe(again)
  if (keyInput()) expect(keyInput()).toBeRequired()
  expect(keyChoices()).toEqual(
    again
      ? ['*Enter a key', 'Use no key']
      : ['*Keep the saved key', 'Replace the saved key', 'Clear the saved key'],
  )
  expect(posted()).toMatchObject({ keyChange: again ? 'replace' : 'keep', etag: 'etag-1' })
})

// Catches: the saved base URL, typed back, still dropping the saved key.
it('keeps the saved key again when the saved base URL comes back', async () => {
  render()
  await retype(baseUrl(), 'https://other.example.com')
  await retype(baseUrl(), model.values.baseUrl)
  expect(posted()).toMatchObject({ keyChange: 'keep' })
  expect(posted()).not.toHaveProperty('apiKey')
})

// Catches: a form that says nothing of the key, or that shows one, and a choice that posts
// something else.
it.each([
  ['Keep the saved key', { keyChange: 'keep' }, false],
  ['Replace the saved key', { keyChange: 'replace', apiKey: '' }, true],
  ['Clear the saved key', { keyChange: 'clear' }, false],
])('posts "%s"', async (choice, fields, asks) => {
  render()
  expect(screen.getByText('An API key is set.')).toBeInTheDocument()
  await userEvent.click(screen.getByRole('radio', { name: choice }))
  expect(keyInput() !== null).toBe(asks)
  expect(posted()).toMatchObject(fields)
  expect('apiKey' in posted()).toBe(asks)
})

// Catches: a model that takes no key asked for one at each save, and a first model that posts
// an etag, or that can keep a key it never had.
it.each([
  ['a model with no key', { ...model, hasApiKey: false }, ['Enter a key', '*Use no key']],
  ['no model', null, ['*Enter a key', 'Use no key']],
])('asks for a key on %s', (_, current, choices) => {
  render({ model: current })
  expect(keyChoices()).toEqual(choices)
  expect(screen.queryByText('No API key is set.') !== null).toBe(current !== null)
  expect('etag' in posted()).toBe(current !== null)
  expect(screen.queryByText(turnsOff)).toBeNull()
})

const typed: RefusedSave = {
  values: {
    protocol: 'openai_responses',
    baseUrl: 'https://typed.example.com/v1',
    model: 'typed-2',
    contextWindow: '9',
    requestArgs: '{ "stream": true }',
  },
  keyChange: 'replace',
  message: 'Correct this.',
}

// Catches: a refusal that is not at its field, one that a screen reader does not hear with the
// field, and one that loses what the person typed or keeps the key in the page.
it.each([
  ['baseUrl', /^Base URL/],
  ['model', /^Model name/],
  ['apiKey', /^Key to save/],
  ['contextWindow', /^Context window/],
  ['requestArgs', /^Arguments/],
] as const)('shows a refusal at %s and keeps what the person typed', (field, label) => {
  render({ refused: { ...typed, field } })
  const input = screen.getByLabelText(label)
  expect(input).toBeInvalid()
  expect(input).toHaveFocus()
  expect(screen.getAllByRole('alert').map((alert) => alert.textContent)).toEqual(['Correct this.'])
  const description = input
    .getAttribute('aria-describedby')!
    .split(' ')
    .map((id) => document.getElementById(id)?.textContent ?? '')
  expect(description[0]).toBe('Correct this.')
  expect(posted()).toMatchObject({ ...typed.values, keyChange: 'replace', apiKey: '' })
  expect(keyInput()).toBeRequired()
})

// Catches: a retry that keeps a key the person cleared or replaced, and a person who typed a key
// not told to type it again, or told so though they typed none.
it.each(['replace', 'clear', 'keep'] as const)(
  'after a refused save, still has the choice to %s the key',
  async (keyChange) => {
    render({ refused: { ...typed, values: model.values, keyChange, field: 'model' } })
    expect(posted()).toMatchObject({ keyChange })
    await userEvent.click(screen.getByRole('radio', { name: 'Replace the saved key' }))
    expect(screen.queryByText('Enter the key again.') !== null).toBe(keyChange === 'replace')
  },
)

// Catches: a save that lost to another person's change replacing it unseen, with the values
// that are saved now hidden or the old etag posted.
it('shows what is saved now beside what the person typed after a conflict', () => {
  render({
    model: { ...model, values: { ...model.values, contextWindow: '' }, etag: 'etag-2' },
    refused: { ...typed, keyChange: 'keep', message: 'Another person changed it.', conflict: true },
  })
  expect(screen.getByRole('alert')).toHaveTextContent('Another person changed it.')
  expect(screen.getAllByText(/^Saved value:/).map((value) => value.textContent)).toEqual([
    'Saved value: Messages (Anthropic)',
    'Saved value: https://api.example.com/v1',
    'Saved value: large-1',
    'Saved value: none',
    'Saved value: {\n  "max_tokens": 4096\n}',
  ])
  expect(posted()).toMatchObject({ ...typed.values, etag: 'etag-2' })
})

// Catches: a page that reloads its data behind the form, as a refused removal does, moving the
// form to a change that the person did not see.
it('stays on the model it opened with while the page reloads behind it', async () => {
  const page = renderLivePage<Page>((p) => <SubAgentPage {...p} />, { model, form })
  await retype(screen.getByLabelText(/^Model name/), 'typed-2')
  await page.update({ model: { ...model, etag: 'etag-2' }, form })
  expect(posted()).toMatchObject({ model: 'typed-2', etag: 'etag-1' })
})

// Catches: the settings of a removed model left in the form that sets the next one.
it('starts empty after the model is removed', async () => {
  const page = renderLivePage<Page>((p) => <SubAgentPage {...p} />, { model, form })
  await page.update({ model: null, form })
  expect(posted()).toMatchObject({ baseUrl: '', model: '', contextWindow: '', requestArgs: '' })
  expect(posted()).not.toHaveProperty('etag')
  expect(screen.queryByRole('button', { name: 'Remove model' })).toBeNull()
})

// Catches: the page saying nothing of who changed the model after that person left.
it.each([
  [model.changedBy, 'Last changed by Ada Lovelace on 2026-11-20 14:03 UTC.'],
  [null, 'Last changed by a former member on 2026-11-20 14:03 UTC.'],
])('says who changed the model last', (changedBy, text) => {
  render({ model: { ...model, changedBy } })
  expect(
    screen.getByText((_, e) => e?.tagName === 'P' && e.textContent === text),
  ).toBeInTheDocument()
})

// Catches: a removal without its consequence, or of another version of the model.
it('asks before it removes the model', async () => {
  render()
  await userEvent.click(screen.getByRole('button', { name: 'Remove model' }))
  const dialog = await screen.findByRole('dialog', { name: 'Remove model' })
  expect(dialog).toHaveTextContent('The sub-agent stops in every workspace.')
  const submit = within(dialog).getByRole<HTMLButtonElement>('button', { name: 'Remove model' })
  expect(Object.fromEntries(new FormData(submit.form!))).toMatchObject({
    intent: 'remove',
    etag: 'etag-1',
  })
})
