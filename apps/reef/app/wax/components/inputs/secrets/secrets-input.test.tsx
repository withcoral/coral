import { createRoutesStub } from 'react-router'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import { SecretsInput } from './secrets-input'

describe('SecretsInput', () => {
  it('masks entire content when no secrets specified', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content="hello world" />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect.element(screen.getByText('***********')).toBeInTheDocument()
  })

  it('masks specified secrets with asterisks', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content="hello secret world" secrets={[[6, 12]]} />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect.element(screen.getByText('hello ****** world')).toBeInTheDocument()
  })

  it('masks multiple secrets', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => (
          <SecretsInput
            content="hello secret1 and secret2 world"
            secrets={[
              [6, 13],
              [18, 25],
            ]}
          />
        ),
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect.element(screen.getByText('hello ******* and ******* world')).toBeInTheDocument()
  })

  it('reveals secrets when eye button is clicked', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content="hello secret world" secrets={[[6, 12]]} />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect.element(screen.getByText('hello ****** world')).toBeInTheDocument()

    const revealButton = screen.getByRole('button', { name: 'Reveal secret' })
    await revealButton.click()

    await expect.element(screen.getByText('hello secret world')).toBeInTheDocument()
  })

  it('hides secrets when eye-off button is clicked', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content="hello secret world" secrets={[[6, 12]]} />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    const revealButton = screen.getByRole('button', { name: 'Reveal secret' })
    await revealButton.click()

    await expect.element(screen.getByText('hello secret world')).toBeInTheDocument()

    const hideButton = screen.getByRole('button', { name: 'Hide secret' })
    await hideButton.click()

    await expect.element(screen.getByText('hello ****** world')).toBeInTheDocument()
  })

  it('reveals full content when no secrets specified and eye clicked', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content="my-api-key" />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect.element(screen.getByText('**********')).toBeInTheDocument()

    const revealButton = screen.getByRole('button', { name: 'Reveal secret' })
    await revealButton.click()

    await expect.element(screen.getByText('my-api-key')).toBeInTheDocument()
  })

  it('handles overlapping secrets correctly', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => (
          <SecretsInput
            content="hello overlapping secrets"
            secrets={[
              [6, 12],
              [10, 16],
            ]}
          />
        ),
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect.element(screen.getByText('hello **********g secrets')).toBeInTheDocument()
  })

  it('handles secrets at the beginning of content', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content="secret at start" secrets={[[0, 6]]} />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect.element(screen.getByText('****** at start')).toBeInTheDocument()
  })

  it('handles secrets at the end of content', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content="ends with secret" secrets={[[10, 16]]} />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect.element(screen.getByText('ends with ******')).toBeInTheDocument()
  })

  it('applies custom className', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput className="custom-class" content="test" />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    const container = screen.container.querySelector('.custom-class')
    expect(container).toBeTruthy()
  })

  it('handles empty content', async () => {
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content="" />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    const revealButton = screen.getByRole('button', { name: 'Reveal secret' })
    expect(revealButton).toBeTruthy()
  })

  it('handles URL-like content with key parameter', async () => {
    const content = 'https://example.com/events/webhook/aws/?key=abc123def456'
    const keyStart = content.indexOf('key=') + 4
    const Stub = createRoutesStub([
      {
        Component: () => <SecretsInput content={content} secrets={[[keyStart, content.length]]} />,
        path: '/',
      },
    ])
    const screen = await render(<Stub />)

    await expect
      .element(screen.getByText('https://example.com/events/webhook/aws/?key=************'))
      .toBeInTheDocument()
  })
})
