import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub } from 'react-router'
import { userEvent, within } from 'storybook/test'
import { ModelTestOutcome } from './model-test'
import { SubAgentPage } from './sub-agent-page'
import type { ModelTestResult, RefusedSave, SubAgentModelView } from './types'

const model: SubAgentModelView = {
  values: {
    protocol: 'anthropic',
    baseUrl: 'https://api.anthropic.com/v1',
    model: 'large-1',
    contextWindow: '200000',
    requestArgs: '{\n  "max_tokens": 4096,\n  "temperature": 0.2\n}',
  },
  hasApiKey: true,
  changedBy: {
    user: 'users/alice',
    name: 'Alice Chen',
    email: 'alice@example.com',
    local: false,
  },
  changedAt: '2026-11-20T14:03:00Z',
  etag: 'model-etag',
}
// What a person typed before Coral refused the save. Their key is not part of it.
const typed: RefusedSave = {
  values: {
    protocol: 'chat_completions',
    baseUrl: 'https://models.example.com/v1',
    model: 'small-2',
    contextWindow: '9000',
    requestArgs: '{\n  "stream": true\n}',
  },
  keyChange: 'replace',
  message: '',
}

const testResults: ModelTestResult[] = [
  { outcome: 'answered' },
  { outcome: 'unreachable' },
  { outcome: 'refused', status: 401 },
  { outcome: 'refused', status: 404 },
  { outcome: 'refused', status: 400 },
  { outcome: 'refused', status: 503 },
  { outcome: 'forbidden' },
  { outcome: 'unreadable' },
  { outcome: 'unknown' },
  { outcome: 'failed', message: 'Coral is not available.' },
]

const meta = {
  title: 'Reef/Sub-agent',
  component: SubAgentPage,
  args: { model, form: { csrf: 'storybook-csrf', requestId: 'storybook-request' } },
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story) => {
      const Stub = createRoutesStub([{ path: '*', Component: () => <Story /> }])
      // The app shell bounds a page's height, so the page's scroll area scrolls.
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <Stub />
        </div>
      )
    },
  ],
} satisfies Meta<typeof SubAgentPage>
export default meta
type Story = StoryObj<typeof meta>

/** The organization has no model, so the form sets one. */
export const NotSet: Story = { args: { model: null } }

/** A saved key is not shown. The form keeps it unless you choose otherwise. */
export const Saved: Story = {}

/** An endpoint that takes no key, changed by a person who left. */
export const SavedWithNoKey: Story = {
  args: {
    model: {
      ...model,
      values: { ...typed.values, contextWindow: '', requestArgs: '' },
      hasApiKey: false,
      changedBy: null,
    },
  },
}

export const ReplacingTheKey: Story = {
  play: async ({ canvasElement }) => {
    await userEvent.click(within(canvasElement).getByRole('button', { name: 'Replace' }))
  },
}

/** A known service fills in the protocol and the base URL. */
export const KnownService: Story = {
  args: { model: null },
  play: async ({ canvasElement }) => {
    await userEvent.click(within(canvasElement).getByRole('radio', { name: 'OpenAI' }))
  },
}

/** A new base URL asks for the key again and says what the save blocks. */
export const NewBaseUrl: Story = {
  play: async ({ canvasElement }) => {
    await userEvent.click(within(canvasElement).getByRole('radio', { name: 'Other' }))
    const baseUrl = within(canvasElement).getByLabelText(/^Base URL/)
    await userEvent.clear(baseUrl)
    await userEvent.type(baseUrl, typed.values.baseUrl)
  },
}

/** The error is at its field, and the key must be typed again. */
export const BaseUrlRefused: Story = {
  args: {
    model: null,
    refused: {
      ...typed,
      field: 'baseUrl',
      message: 'This deployment does not permit this URL. Enter a different URL.',
    },
  },
}

export const ArgumentsRefused: Story = {
  args: {
    refused: {
      ...typed,
      field: 'requestArgs',
      message: 'Remove each name that is not in the list below.',
    },
  },
}

export const KeyRefused: Story = {
  args: {
    refused: {
      ...typed,
      field: 'apiKey',
      message: 'Enter an API key without line breaks or other control characters.',
    },
  },
}

/** Another person changed the model first, so each field says what is saved now. */
export const StaleSave: Story = {
  args: {
    refused: {
      ...typed,
      conflict: true,
      message:
        'Another person changed the model after you opened this page. Compare your values with the saved values. Save again to replace the saved values.',
    },
  },
}

export const CoralUnavailable: Story = {
  args: {
    refused: { ...typed, message: 'Coral is not available.' },
  },
}

/** Each way a test of the model can end. A refused setting shows at its field instead. */
export const TestOutcomes: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, maxWidth: 640, padding: 24 }}>
      {testResults.map((result) => (
        <ModelTestOutcome key={JSON.stringify(result)} result={result} />
      ))}
    </div>
  ),
}
