import { useId, useState, type ReactNode } from 'react'
import { Banner } from '@/wax/components'
import { testOutcomeMessage } from './copy'
import type { ModelTestResult, SettingRefusal } from './types'

const TEST_PATH = '/organization/sub-agent/test'

interface TestState {
  /** The test that the frame is for. Each has a frame of its own. */
  number: number
  /** Undefined while there is nothing to say, and null while the model is asked. */
  result: ModelTestResult | null | undefined
}

/**
 * A test of what is in the page's form. The browser itself submits the form, into a frame of the
 * page: the page stays, with the typed API key in its input, and the key reaches neither React
 * nor the router. A frame's first document replaces the empty one it starts with, so a frame for
 * each test leaves nothing in the history.
 */
export function useModelTest() {
  // A frame name cannot start with an underscore, as React's ids do.
  const frameName = `model-test${useId()}`
  const [test, setTest] = useState<TestState>({ number: 0, result: undefined })
  const testing = test.result === null
  const onLoad = (frame: HTMLIFrameElement) => {
    if (!testing) return
    const result = resultIn(frame)
    if (result !== undefined) setTest({ number: test.number + 1, result })
  }
  return {
    testing,
    /** Differs for each result. */
    number: test.number,
    result: test.result ?? undefined,
    /** A test started, so it has taken the place of what a refused save said. */
    tried: test.number > 0 || testing,
    /** For the button that submits the form as a test. */
    button: { formAction: TEST_PATH, formTarget: frameName },
    start: () => setTest((last) => ({ ...last, result: null })),
    /** Forgets the outcome, or leaves the test that runs. It was not about what is typed now. */
    clear: () =>
      setTest((last) =>
        last.result === undefined ? last : { number: last.number + 1, result: undefined },
      ),
    frame: (
      <iframe
        key={test.number}
        name={frameName}
        title="Model test"
        hidden
        // Whatever the frame loads, such as a sign-in page, it runs no script.
        sandbox="allow-same-origin"
        onLoad={(event) => onLoad(event.currentTarget)}
      />
    ),
  }
}

/** The result in a frame that loaded, or undefined for the empty document it starts with. */
function resultIn(frame: HTMLIFrameElement): ModelTestResult | undefined {
  try {
    if (frame.contentWindow!.location.href === 'about:blank') return undefined
    const result = JSON.parse(frame.contentDocument!.body.textContent) as ModelTestResult
    if (typeof result.outcome === 'string') return result
  } catch {
    // Not the test's answer: a page of another server, or a sign-in page.
  }
  return { outcome: 'failed', message: 'Reload the page before you test again.' }
}

/** What a test showed, in words. A refused setting shows at its field instead. */
export function ModelTestOutcome({ result }: { result: ModelTestResult }): ReactNode {
  if (result.outcome === 'invalid') return null
  return (
    <Banner variant={result.outcome === 'answered' ? 'success' : 'error'}>
      {testOutcomeMessage(result)}
    </Banner>
  )
}

/** The field that a test refused, to show the reason there. */
export function testRefusal(result: ModelTestResult | undefined): SettingRefusal | undefined {
  return result?.outcome === 'invalid' ? result : undefined
}
