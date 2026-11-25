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
const keyInput = () => screen.queryByLabelText(/^API key/, { selector: 'input' })
const button = (name: string) => screen.getByRole('button', { name })
const pick = (service: string) => userEvent.click(screen.getByRole('radio', { name: service }))
/** What Save posts. */
const posted = () => {
  const save = screen.getByRole<HTMLButtonElement>('button', { name: 'Save' })
  return Object.fromEntries(new FormData(save.form!))
}
const retype = async (input: HTMLElement, text: string) => {
  await userEvent.clear(input)
  await userEvent.type(input, text)
}

const blocks = 'Saving a new base URL blocks the sub-agent in every workspace.'

// Catches: a new base URL that drops the saved key, or blocks the sub-agent, without a word,
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
  expect(screen.queryByText(blocks) !== null).toBe(again)
  expect(keyInput() !== null).toBe(again)
  if (keyInput()) expect(keyInput()).toBeRequired()
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

// Catches: a form that says nothing of the key, or that shows one, a choice that posts something
// else, and one that a person cannot take back.
it.each([
  ['Replace', { keyChange: 'replace', apiKey: '' }, true],
  ['Remove', { keyChange: 'clear' }, false],
])('posts "%s" for the saved key, until the person keeps it', async (choice, fields, asks) => {
  render()
  expect(screen.getByRole('group', { name: 'API key' })).toHaveTextContent('A key is saved.')
  expect(posted()).toMatchObject({ keyChange: 'keep' })
  await userEvent.click(button(choice))
  expect(keyInput() !== null).toBe(asks)
  expect(posted()).toMatchObject(fields)
  expect('apiKey' in posted()).toBe(asks)
  await userEvent.click(button('Keep the saved key'))
  expect(posted()).toMatchObject({ keyChange: 'keep' })
  expect(posted()).not.toHaveProperty('apiKey')
})

// Catches: a server that takes no key asked for one at each save, and a first model that posts an
// etag, or that can keep a key it never had.
it.each([
  ['a model with no key', { ...model, hasApiKey: false }],
  ['no model', null],
])('lets a person leave the key empty on %s', (_, current) => {
  render({ model: current })
  expect(keyInput()).not.toBeRequired()
  expect(posted()).toMatchObject({ keyChange: 'optional', apiKey: '' })
  expect(screen.queryByRole('button', { name: 'Keep the saved key' })).toBeNull()
  expect('etag' in posted()).toBe(current !== null)
  expect(screen.queryByText(blocks)).toBeNull()
})

// Catches: a saved key dropped by a new base URL with no way to say that the server takes none.
it('lets a person use no key for a new base URL', async () => {
  render()
  await retype(baseUrl(), 'https://other.example.com')
  await userEvent.click(button('Use no key'))
  expect(posted()).toMatchObject({ keyChange: 'clear' })
  expect(posted()).not.toHaveProperty('apiKey')
  await userEvent.click(button('Enter a key'))
  expect(keyInput()).toBeRequired()
})

// Catches: a service that posts another host or protocol than the page shows, and a host that
// nobody entered.
it.each([
  ['Anthropic', 'anthropic', 'https://api.anthropic.com/v1'],
  ['OpenAI', 'openai_responses', 'https://api.openai.com/v1'],
])('fills in where %s serves its models', async (service, protocol, url) => {
  render({ model: null })
  expect(posted()).toMatchObject({ baseUrl: '' })
  await pick(service)
  expect(screen.getByText(url)).toBeInTheDocument()
  expect(screen.queryByLabelText(/^Base URL/)).toBeNull()
  expect(posted()).toMatchObject({ protocol, baseUrl: url })
  // It takes a key, so there is no way past one.
  expect(keyInput()).toBeRequired()
  expect(screen.queryByRole('button', { name: 'Use no key' })).toBeNull()
  await pick('Other')
  expect(posted()).toMatchObject({ protocol: 'chat_completions', baseUrl: '' })
})

// Catches: a change of service, which changes the host, that blocks the sub-agent without a word.
it('warns when another service is chosen, and opens on the service that is saved', async () => {
  render({
    model: {
      ...model,
      values: { ...model.values, baseUrl: 'https://api.anthropic.com/v1' },
    },
  })
  expect(screen.getByRole('radio', { name: 'Anthropic' })).toBeChecked()
  await pick('OpenAI')
  expect(screen.getByText(blocks)).toBeInTheDocument()
  expect(keyInput()).toBeRequired()
  await pick('Anthropic')
  expect(screen.queryByText(blocks)).toBeNull()
  expect(posted()).toMatchObject({ keyChange: 'keep' })
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

// Catches: advanced settings in the way of a person who does not change them, or closed over one to
// correct, or over saved values to compare with.
it.each([
  ['no model', { model: null }, false],
  ['a model that has them', {}, false],
  ['a refusal of another field', { refused: { ...typed, field: 'model' } }, false],
  ['a refused context window', { refused: { ...typed, field: 'contextWindow' } }, true],
  ['refused arguments', { refused: { ...typed, field: 'requestArgs' } }, true],
  ['a conflict', { refused: { ...typed, conflict: true } }, true],
] as const)('with %s, opens the advanced settings only if it must', (_, props, open) => {
  render(props)
  expect(screen.getByText('Advanced').closest('details')!.open).toBe(open)
})

// Catches: a page that does not say what the model is for, or what a person must do next.
it.each([
  [null, 'The sub-agent is a model that does data work for your agents.'],
  [model, 'blocked in each workspace until a workspace owner turns it on, on the Tools page.'],
])('says what comes next', (current, text) => {
  render({ model: current })
  expect(screen.getByText(text, { exact: false })).toBeInTheDocument()
})

// Catches: a refusal that is not at its field, one that a screen reader does not hear with the
// field, and one that loses what the person typed or keeps the key in the page.
it.each([
  ['baseUrl', /^Base URL/],
  ['model', /^Model name/],
  ['apiKey', /^API key/],
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

// Catches: a retry that keeps a key the person removed or replaced, and a person who typed a key
// not told to type it again, or told so though they typed none.
it.each(['replace', 'clear', 'keep'] as const)(
  'after a refused save, still has the choice to %s the key',
  async (keyChange) => {
    render({ refused: { ...typed, values: model.values, keyChange, field: 'model' } })
    expect(posted()).toMatchObject({ keyChange })
    if (keyChange === 'keep') await userEvent.click(button('Replace'))
    expect(screen.queryByText('Enter the key again.') !== null).toBe(keyChange === 'replace')
  },
)

// Catches: a first save that Coral refused, whose retry sets the model without the key that the
// person typed.
it('asks again for the key that a refused first save left behind', () => {
  render({ model: null, refused: { ...typed, field: 'model' } })
  expect(keyInput()).toBeRequired()
  expect(screen.getByText('Enter the key again.')).toBeInTheDocument()
})

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
