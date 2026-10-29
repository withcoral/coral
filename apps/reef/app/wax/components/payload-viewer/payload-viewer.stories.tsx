import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, waitFor, within } from 'storybook/test'
import { inTestBrowser, press } from '../../../../.storybook/browser'
import { PayloadViewer } from './payload-viewer'

const largeJson = JSON.stringify({
  items: Array.from({ length: 80 }, (_, index) => ({
    id: index,
    name: `item-${index}`,
    tags: ['a', 'b'],
  })),
  cursor: { next: 'abc' },
  description: 'A long description that folds to its start. '.repeat(10),
})

const meta = {
  title: 'Wax/PayloadViewer',
  component: PayloadViewer,
  args: {
    label: 'Provider response',
    format: 'json',
    value: '{"id":9007199254740993,"ratio":1.0e3,"id":"duplicate","list":[1,true,null],"empty":{}}',
  },
} satisfies Meta<typeof PayloadViewer>
export default meta
type Story = StoryObj<typeof meta>

type Context = Parameters<NonNullable<Story['play']>>[0]
/** The inline payload's structure. */
const structureOf = (canvasElement: HTMLElement) =>
  within(within(canvasElement).getByRole('group', { name: 'Payload content' })).getByRole('group', {
    name: 'Structure',
  })
/** Opens the expanded viewer of the payload named `label`. */
const openExpanded = async ({ canvasElement, userEvent }: Context, label = 'Provider response') => {
  const page = within(canvasElement.ownerDocument.body)
  await userEvent.click(page.getByRole('button', { name: `Expand ${label}` }))
  return page.findByRole('dialog', { name: label })
}
/** The current match's parts in the expanded `viewer`. */
const currentMatch = (viewer: HTMLElement) => within(viewer).getAllByRole('mark', { current: true })

/**
 * A small value opens whole. Catches: a renderer that reparses and reserializes JSON, which
 * rounds large integers, respells numbers, drops duplicate keys and rewrites escapes.
 */
export const SmallJson: Story = {
  args: {
    value:
      '{"id":12345678901234567890,"ratio":1.0e3,"id":"dup","text":"a\\u0041\\n","list":[1,true,null],"empty":{}}',
  },
  play: async ({ canvasElement }) => {
    const tree = structureOf(canvasElement)
    for (const token of ['12345678901234567890', '1.0e3', '"dup"', '"a\\u0041\\n"', 'true', 'null'])
      await expect(tree).toHaveTextContent(token)
    await expect(tree).toHaveTextContent('{}')
    // Both `id` keys stay, in order.
    await expect(tree.textContent!.match(/"id": /g)).toHaveLength(2)
    // Nothing starts folded, and each container states its size.
    await expect(within(tree).queryAllByRole('button', { expanded: false })).toHaveLength(0)
    await expect(within(tree).getByRole('button', { name: 'list: 3 items' })).toBeVisible()
    await expect(within(tree).getByRole('button', { name: 'Value: 6 properties' })).toBeVisible()
  },
}

const large = JSON.stringify({
  rows: Array.from({ length: 120 }, (_, index) => ({ id: index, name: `row-${index}` })),
  meta: { page: 1 },
  note: 'n'.repeat(400),
})
/**
 * A large value opens shallow; its long string folds to its start. Catches: a large payload
 * that opens as one endless document, or folding that drops content.
 */
export const LargeJson: Story = {
  args: { value: large },
  play: async ({ canvasElement, userEvent }) => {
    const content = within(canvasElement).getByRole('group', { name: 'Payload content' })
    const tree = structureOf(canvasElement)
    // The top level shows; the containers in it fold, each with its size.
    await expect(within(tree).getByRole('button', { name: 'Value: 3 properties' })).toHaveAttribute(
      'aria-expanded',
      'true',
    )
    const rows = within(tree).getByRole('button', { name: 'rows: 120 items' })
    await expect(rows).toHaveAttribute('aria-expanded', 'false')
    await expect(within(tree).getByRole('button', { name: 'meta: 1 property' })).toHaveAttribute(
      'aria-expanded',
      'false',
    )
    await expect(tree).not.toHaveTextContent('row-0')
    // A long string shows its start and folds the rest, which one press shows.
    // 402 characters with its quotes; the first 120 show.
    // Its button says what it does, and its name starts with what it shows.
    const note = within(tree).getByRole('button', { name: 'Show 282 more characters: note' })
    await expect(note).toHaveTextContent(/^Show 282 more characters$/)
    await expect(note).toHaveAttribute('aria-expanded', 'false')
    await expect(tree).not.toHaveTextContent(`"${'n'.repeat(400)}"`)
    await userEvent.click(note)
    await expect(tree).toHaveTextContent(`"${'n'.repeat(400)}"`)
    await expect(note).toHaveAccessibleName('Fold: note')
    // Opened, the list shows its first item, and its last at the card's end.
    await userEvent.click(rows)
    await expect(rows).toHaveAttribute('aria-expanded', 'true')
    await expect(within(tree).getByRole('button', { name: 'Item 1: 2 properties' })).toBeVisible()
    await waitFor(() => {
      content.scrollTop = content.scrollHeight
      insideOf(within(tree).getByRole('button', { name: 'Item 120: 2 properties' }), content)
    })
  },
}
export const LargeJsonNarrow: Story = {
  args: { value: large },
  parameters: { chromatic: { viewports: [390] } },
}

/** Catches: long strings in a small payload that start folded, or fold together. */
export const LongStringsFoldAlone: Story = {
  args: { value: JSON.stringify({ a: 'x'.repeat(201), b: 'x'.repeat(201) }) },
  play: async ({ canvasElement, userEvent }) => {
    const tree = structureOf(canvasElement)
    const [a, b] = [
      within(tree).getByRole('button', { name: 'Fold: a' }),
      within(tree).getByRole('button', { name: 'Fold: b' }),
    ]
    await expect(a).toHaveAttribute('aria-expanded', 'true')
    await userEvent.click(a)
    await expect(a).toHaveAttribute('aria-expanded', 'false')
    await expect(b).toHaveAttribute('aria-expanded', 'true')
  },
}

/** Catches: a Find that searches only what is unfolded, or finds a match it never shows. */
export const FindsInFoldedContent: Story = {
  args: { value: large },
  play: async (context) => {
    const { userEvent } = context
    const viewer = await openExpanded(context)
    const tree = within(viewer).getByRole('group', { name: 'Structure' })
    const find = within(viewer).getByRole('searchbox', { name: 'Find in Provider response' })
    const status = within(viewer).getByRole('status')
    await userEvent.type(find, 'row-11')
    // row-11 and row-110 to row-119.
    await expect(status).toHaveTextContent('1 of 11')
    await waitFor(() => {
      expect(currentMatch(viewer).map((part) => part.textContent)).toEqual(['row-11'])
      insideOf(currentMatch(viewer)[0]!, scrollerOf(tree))
    })
    await expect(within(tree).getByRole('button', { name: 'rows: 120 items' })).toHaveAttribute(
      'aria-expanded',
      'true',
    )
    await userEvent.keyboard('{Enter}')
    await expect(status).toHaveTextContent('2 of 11')
    await waitFor(() =>
      expect(currentMatch(viewer)[0]!.parentElement).toHaveTextContent('"row-110"'),
    )
    await userEvent.keyboard('{Shift>}{Enter}{/Shift}')
    await userEvent.keyboard('{Shift>}{Enter}{/Shift}')
    await expect(status).toHaveTextContent('11 of 11')
    await waitFor(() => insideOf(currentMatch(viewer)[0]!, scrollerOf(tree)))
    await userEvent.click(within(viewer).getByRole('button', { name: 'Next match' }))
    await expect(status).toHaveTextContent('1 of 11')
    // The folded end of a long string is searched too, and unfolds.
    await userEvent.clear(find)
    await userEvent.type(find, 'nnn$')
    await expect(status).toHaveTextContent('1 of 1')
    await waitFor(() =>
      expect(within(tree).getByRole('button', { name: 'Fold: note' })).toHaveAttribute(
        'aria-expanded',
        'true',
      ),
    )
    await userEvent.clear(find)
    await userEvent.type(find, 'absent')
    await expect(status).toHaveTextContent('No matches')
    await expect(within(viewer).getByRole('button', { name: 'Next match' })).toBeDisabled()
  },
}

/**
 * Catches: an expanded viewer that opens folded again, or that leaves the dialog's height
 * unused. It opens with the folds you had, its body filling the dialog.
 */
export const ExpandedOpensOnYourView: Story = {
  args: { value: large },
  play: async (context) => {
    const tree = structureOf(context.canvasElement)
    await context.userEvent.click(within(tree).getByRole('button', { name: 'rows: 120 items' }))
    const viewer = await openExpanded(context)
    const shown = within(viewer).getByRole('group', { name: 'Structure' })
    await expect(within(shown).getByRole('button', { name: 'rows: 120 items' })).toHaveAttribute(
      'aria-expanded',
      'true',
    )
    await expect(
      viewer.getBoundingClientRect().bottom - scrollerOf(shown).getBoundingClientRect().bottom,
    ).toBeLessThan(48)
  },
}

/** Find's status counts what Raw alone shows, beside what it shows here or instead of it. */
const findStatus = (value: string, query: string, status: string): Story => ({
  args: { value },
  play: async (context) => {
    const viewer = await findExpanded(context, 'Provider response', query)
    await expect(within(viewer).getByRole('status').textContent).toBe(status)
  },
})
export const FindSaysWhatOnlyRawShows = findStatus('[1,2]', '1,2', 'No matches · 1 in Raw')
export const FindSaysWhatRawAlsoShows = findStatus(
  '{"a":"1,2","b":[1,2]}',
  '1,2',
  '1 of 1 · 1 more in Raw',
)

/** Catches: a view switch that keeps a match index but leaves the match folded away. */
export const RevealsTheMatchAfterAViewSwitch: Story = {
  args: { value: large },
  play: async (context) => {
    const { userEvent } = context
    const viewer = await openExpanded(context)
    await userEvent.click(within(viewer).getByRole('radio', { name: 'Raw' }))
    await userEvent.type(within(viewer).getByRole('searchbox'), 'row-77')
    await userEvent.click(within(viewer).getByRole('radio', { name: 'Structured' }))
    await expect(within(viewer).getByRole('status')).toHaveTextContent('1 of 1')
    const tree = within(viewer).getByRole('group', { name: 'Structure' })
    await waitFor(() => {
      expect(currentMatch(viewer).map((part) => part.textContent)).toEqual(['row-77'])
      insideOf(currentMatch(viewer)[0]!, scrollerOf(tree))
    })
  },
}

/** Raw shows `value` exactly as retained, every line terminator included. */
const exactInRaw = (value: string): Story => ({
  args: { value },
  play: async (context) => {
    const viewer = await openExpanded(context)
    await context.userEvent.click(within(viewer).getByRole('radio', { name: 'Raw' }))
    await expect(within(viewer).queryByRole('group', { name: 'Structure' })).toBeNull()
    await expect(within(viewer).getByRole('group', { name: 'Raw' }).textContent).toBe(value)
  },
})
export const RawKeepsPreciseNumbers = exactInRaw('{"n":12345678901234567890.000,"e":-0E+2}')
export const RawKeepsDuplicateKeys = exactInRaw('{"a":1,"a":2}')
export const RawKeepsAScalarsSpace = exactInRaw('  "spaced"\n')
export const RawKeepsLineTerminators = exactInRaw('{"a":\r\n"x",\r"b":\n"x"}')

/** A log keeps its lines and whitespace. Catches: a log shown as JSON, or collapsed whitespace. */
export const TextLog: Story = {
  args: {
    label: 'Log message',
    format: 'text',
    value: 'first  line\n\tindented\n\nlast {"not":"json"}',
  },
  play: async (context) => {
    const { canvasElement, userEvent } = context
    const shown = `1first  line2\tindented34last {"not":"json"}`
    await expect(within(canvasElement).getByRole('group', { name: 'Lines' }).textContent).toBe(
      shown,
    )
    const viewer = await openExpanded(context, 'Log message')
    await expect(within(viewer).getByRole('group', { name: 'Lines' }).textContent).toBe(shown)
    await expect(within(viewer).queryByRole('radiogroup', { name: 'View' })).toBeNull()
    await expect(within(viewer).queryByRole('group', { name: 'Structure' })).toBeNull()
    const wrap = within(viewer).getByRole('button', { name: 'Wrap' })
    await expect(wrap).toHaveAttribute('aria-pressed', 'true')
    await userEvent.click(wrap)
    await expect(wrap).toHaveAttribute('aria-pressed', 'false')
  },
}
/** Catches: malformed JSON that is reformatted, hidden, or offered a structure it lacks. */
export const MalformedJson: Story = {
  args: { value: '{"status": 200, "data": [1, 2' },
  play: async (context) => {
    const value = '{"status": 200, "data": [1, 2'
    await expect(
      within(context.canvasElement).getByRole('group', { name: 'Raw' }).textContent,
    ).toBe(value)
    const viewer = await openExpanded(context)
    await expect(within(viewer).getByRole('group', { name: 'Raw' }).textContent).toBe(value)
    await expect(within(viewer).queryByRole('radiogroup', { name: 'View' })).toBeNull()
    await expect(within(viewer).getByRole('group', { name: 'Payload tools' })).toHaveTextContent(
      'Not valid JSON',
    )
  },
}
/** Catches: JSON nested deep enough to exhaust the stack when its structure is walked. */
export const VeryDeepJson: Story = {
  args: { value: '['.repeat(5000) + ']'.repeat(5000) },
  play: async (context) => {
    const viewer = await openExpanded(context)
    await expect(within(viewer).queryByRole('radiogroup', { name: 'View' })).toBeNull()
    await expect(within(viewer).getByRole('group', { name: 'Raw' }).textContent).toBe(
      '['.repeat(5000) + ']'.repeat(5000),
    )
  },
}
/** Catches: JSON null shown as nothing. */
export const JsonNull: Story = {
  args: { label: 'Final value', value: 'null' },
  play: async ({ canvasElement }) => {
    await expect(structureOf(canvasElement).textContent).toBe('null')
  },
}
export const Truncated: Story = {
  args: { label: 'Upstream response data', value: '{"message":"Too many', truncated: true },
}

/** A short, narrow phone screen. Chromatic reads `viewports`; Vitest reads the global. */
const narrow = {
  globals: { viewport: { value: 'mobile1', isRotated: false } },
  parameters: { chromatic: { viewports: [320] } },
}
const tallJson = JSON.stringify(
  Object.fromEntries(Array.from({ length: 40 }, (_, index) => [`key${index}`, `value ${index}`])),
)
const tallText = Array.from({ length: 60 }, (_, index) => `log line ${index + 1}`).join('\n')
const tallMalformed = `${Array.from({ length: 60 }, (_, index) => `"line ${index}",`).join('\n')} [`

/**
 * Catches: an inline payload with no working height bound (an invalid max-height computes to
 * none), or actions that take room inside its card. The actions sit in a compact row above
 * the bordered payload, at its right; the payload takes the full width and scrolls in itself.
 */
const bounded: Story['play'] = async ({ canvasElement, globals }) => {
  const region = within(canvasElement).getByRole('region')
  const content = within(region).getByRole('group', { name: 'Payload content' })
  const actions = within(region).getByRole('group', { name: 'Payload actions' })
  const view = canvasElement.ownerDocument.defaultView!
  const bound = Math.min(320, view.innerHeight / 2)
  // A narrow story runs on the short phone screen, where half the screen is the bound.
  if (globals.viewport?.value === 'mobile1') {
    await expect(view.innerWidth).toBeLessThanOrEqual(360)
    await expect(bound).toBeLessThan(320)
  }
  const maxHeight = getComputedStyle(content).maxHeight
  await expect(maxHeight).toMatch(/^[\d.]+px$/)
  await expect(parseFloat(maxHeight)).toBeCloseTo(bound, 0)
  await expect(content.getBoundingClientRect().height).toBeLessThanOrEqual(bound + 0.5)
  await expect(content.scrollHeight).toBeGreaterThan(content.clientHeight)
  // The actions sit above the bordered payload, right-aligned with it, not inside it.
  const [regionBox, actionsBox, contentBox] = [region, actions, content].map((element) =>
    element.getBoundingClientRect(),
  )
  await expect(content.contains(actions)).toBe(false)
  await expect(getComputedStyle(content).borderTopStyle).toBe('solid')
  await expect(actionsBox.bottom).toBeLessThanOrEqual(contentBox.top)
  await expect(Math.abs(contentBox.right - actionsBox.right)).toBeLessThan(4)
  await expect(actionsBox.left).toBeGreaterThan(contentBox.left + contentBox.width / 2)
  await expect(actionsBox.height).toBeLessThan(40)
  // The payload takes the full width.
  await expect(contentBox.width).toBeGreaterThanOrEqual(regionBox.width - 1)
  // It scrolls inside itself; the actions stay where they were.
  content.scrollTop = 160
  await waitFor(() => expect(content.scrollTop).toBeGreaterThan(0))
  await expect(actions.getBoundingClientRect().top).toBe(actionsBox.top)
}
export const BoundedJson: Story = { args: { value: tallJson }, play: bounded }
export const BoundedJsonNarrow: Story = { ...BoundedJson, ...narrow }
export const BoundedRaw: Story = { args: { value: tallMalformed }, play: bounded }
export const BoundedRawNarrow: Story = { ...BoundedRaw, ...narrow }
export const BoundedText: Story = {
  args: { label: 'Log message', format: 'text', value: tallText },
  play: bounded,
}
export const BoundedTextNarrow: Story = { ...BoundedText, ...narrow }

/**
 * Catches: an expanded viewer whose close control leaves the standard Wax header: it sits on
 * the title's row at the top right, the title at the left. Escape and the close control both
 * close it, and focus returns to Expand.
 */
const expandedHeader: Story['play'] = async ({ canvasElement, userEvent, globals }) => {
  const page = within(canvasElement.ownerDocument.body)
  if (globals.viewport?.value === 'mobile1')
    await expect(canvasElement.ownerDocument.defaultView!.innerWidth).toBeLessThanOrEqual(360)
  const expand = within(canvasElement).getByRole('button', { name: 'Expand Provider response' })
  expand.focus()
  await userEvent.keyboard('{Enter}')
  const dialog = await page.findByRole('dialog', { name: 'Provider response' })
  const title = within(dialog).getByRole('heading', { name: 'Provider response' })
  const close = within(dialog).getByRole('button', { name: 'Close' })
  await waitFor(() => expect(close).toBeVisible())
  const [dialogBox, titleBox, closeBox] = [dialog, title, close].map((element) =>
    element.getBoundingClientRect(),
  )
  // One row: the close control's centre falls within the title's line.
  const middle = closeBox.top + closeBox.height / 2
  await expect(middle).toBeGreaterThanOrEqual(titleBox.top)
  await expect(middle).toBeLessThanOrEqual(titleBox.bottom)
  // Title at the left, close control at the right, apart.
  await expect(titleBox.left - dialogBox.left).toBeLessThan(40)
  await expect(dialogBox.right - closeBox.right).toBeLessThan(40)
  await expect(closeBox.left).toBeGreaterThan(titleBox.right)
  // Above the tools.
  const tools = within(dialog).getByRole('group', { name: 'Payload tools' })
  await expect(closeBox.bottom).toBeLessThanOrEqual(tools.getBoundingClientRect().top)
  // Tab and Shift+Tab cycle inside the dialog; focus never reaches the page behind it.
  const focusables = dialog.querySelectorAll('button, input, [tabindex="0"]').length
  for (const key of ['{Tab}', '{Shift>}{Tab}{/Shift}'])
    for (let tabs = 0; tabs <= focusables; tabs++) {
      await userEvent.keyboard(key)
      // Base UI's focus guard at either end hands focus back into the dialog.
      await waitFor(() =>
        expect(dialog).toContainElement(canvasElement.ownerDocument.activeElement as HTMLElement),
      )
    }
  await userEvent.keyboard('{Escape}')
  await waitFor(() => expect(page.queryByRole('dialog')).toBeNull())
  await waitFor(() => expect(expand).toHaveFocus())
  // The close control closes it too, and focus returns the same way.
  await userEvent.click(expand)
  await userEvent.click(
    within(await page.findByRole('dialog', { name: 'Provider response' })).getByRole('button', {
      name: 'Close',
    }),
  )
  await waitFor(() => expect(page.queryByRole('dialog')).toBeNull())
  await waitFor(() => expect(expand).toHaveFocus())
}
export const ExpandedHeader: Story = { args: { value: largeJson }, play: expandedHeader }
export const ExpandedHeaderNarrow: Story = { ...ExpandedHeader, ...narrow }

// Moderate payloads, in Chromium: every row mounts, so the page's own find, a screen reader's
// reading and a selection by hand reach all of it.
const moderateJson = JSON.stringify(
  Object.fromEntries(
    Array.from({ length: 30 }, (_, index) => [
      `k${String(index).padStart(2, '0')}`,
      { a: index, b: `value ${index}` },
    ]),
  ),
)
const moderateText = Array.from({ length: 200 }, (_, index) => `log line ${index + 1}`).join('\n')
const moderateRaw = JSON.stringify({ ...JSON.parse(moderateJson), note: 'n'.repeat(800) }, null, 2)
/** What a selection of all of `element` by hand holds. */
const selected = (element: Element) => {
  const selection = element.ownerDocument.getSelection()!
  selection.selectAllChildren(element)
  const text = selection.toString()
  selection.removeAllRanges()
  return text
}

/** Catches: a moderate structure that mounts only its rows near the view. */
export const ModerateJsonMountsWhole: Story = {
  args: { value: moderateJson },
  play: async ({ canvasElement }) => {
    const tree = structureOf(canvasElement)
    await expect(
      within(tree).getAllByRole('button', { name: /^k\d\d: 2 properties$/ }),
    ).toHaveLength(30)
    const text = selected(tree)
    await expect(text).toContain('"k00": {')
    await expect(text).toContain('"b": "value 29"')
  },
}

/** Catches: a moderate log that mounts only its lines near the view. */
export const ModerateTextMountsWhole: Story = {
  args: { label: 'Log message', format: 'text', value: moderateText },
  play: async ({ canvasElement }) => {
    const lines = within(canvasElement).getByRole('group', { name: 'Lines' })
    const text = selected(lines)
    await expect(text.match(/log line \d+/g)).toHaveLength(200)
    await expect(text).toContain('log line 200')
  },
}

/**
 * Catches: moderate Raw that mounts only its lines near the view, or splits a long line. All
 * of it mounts exactly, and a selection by hand holds it as retained.
 */
export const ModerateRawMountsWhole: Story = {
  args: { value: moderateRaw },
  play: async (context) => {
    const viewer = await openExpanded(context)
    await context.userEvent.click(within(viewer).getByRole('radio', { name: 'Raw' }))
    const raw = within(viewer).getByRole('group', { name: 'Raw' })
    await expect(raw.textContent).toBe(moderateRaw)
    await expect(selected(raw)).toBe(moderateRaw)
  },
}

/**
 * Catches: an expanded body that Tab never reaches, or that shows no focus. Its toggles are
 * focusable, so the browser does not make the body focusable for you. Tab reaches the named
 * body, it shows its focus, and ArrowDown and PageDown scroll it, pressed as a person would.
 */
export const ExpandedBodyScrollsByKey: Story = {
  args: { value: moderateJson },
  play: async (context) => {
    const viewer = await openExpanded(context)
    const body = within(viewer).getByRole('group', { name: 'Payload content' })
    await expect(body.scrollHeight).toBeGreaterThan(body.clientHeight)
    // Storybook's own UI has no test browser to press keys in.
    if (!(await inTestBrowser())) return
    const focused = () => context.canvasElement.ownerDocument.activeElement
    for (let tabs = 0; tabs < 40 && focused() !== body; tabs++) await press('{Tab}')
    await expect(body).toHaveFocus()
    await expect(getComputedStyle(body).outlineStyle).toBe('solid')
    await press('{ArrowDown}')
    await waitFor(() => expect(body.scrollTop).toBeGreaterThan(0))
    const after = body.scrollTop
    await press('{PageDown}')
    await waitFor(() => expect(body.scrollTop).toBeGreaterThan(after + body.clientHeight / 2))
    await expect(body).toHaveFocus()
  },
}

// Huge payloads, in Chromium: what a scrolling body mounts, and what you reach in it.
const hugeArray = JSON.stringify(
  Array.from({ length: 10_000 }, (_, index) => `item-${String(index).padStart(5, '0')}`),
)
const hugeText = Array.from({ length: 100_000 }, (_, index) => `log line ${index + 1}`).join('\n')
const hugeMalformed = `${Array.from({ length: 50_000 }, (_, index) => `"row ${index + 1}",`).join('\n')} [`
/** How many times `pattern` shows in what `element` mounts. */
const mounted = (element: Element, pattern: RegExp) =>
  (element.textContent ?? '').match(new RegExp(pattern, 'g'))?.length ?? 0
/** The element that scrolls `element`. */
const scrollerOf = (element: Element): HTMLElement => {
  let at = element.parentElement
  while (at && !/auto|scroll/.test(getComputedStyle(at).overflowY)) at = at.parentElement
  return at!
}
const insideOf = (element: Element, frame: Element) => {
  const [box, bounds] = [element, frame].map((each) => each.getBoundingClientRect())
  expect(box.top).toBeGreaterThanOrEqual(bounds.top - 0.5)
  expect(box.bottom).toBeLessThanOrEqual(bounds.bottom + 0.5)
}
/** Scrolls `content` to its end until `text` shows inside it. */
const scrollsTo = (content: HTMLElement, text: string) =>
  waitFor(() => {
    content.scrollTop = content.scrollHeight
    insideOf(within(content).getByText(text), content)
  })
/** Opens the expanded viewer and finds `query` in it. */
const findExpanded = async (context: Context, label: string, query: string) => {
  const viewer = await openExpanded(context, label)
  await context.userEvent.type(within(viewer).getByRole('searchbox'), query)
  return viewer
}

/**
 * Catches: a huge array read through repeated "Show next" chunks, or one that mounts every
 * item. You scroll the card to its last item, and only the items near the view mount.
 */
export const HugeArrayScrolls: Story = {
  args: { value: hugeArray },
  play: async ({ canvasElement }) => {
    const content = within(canvasElement).getByRole('group', { name: 'Payload content' })
    await expect(within(content).queryByRole('button', { name: /^Show next/ })).toBeNull()
    await expect(mounted(content, /item-\d{5}/)).toBeLessThan(400)
    await scrollsTo(content, '"item-09999"')
    await expect(mounted(content, /item-\d{5}/)).toBeLessThan(400)
  },
}

/**
 * Catches: a Find that mounts every item before the one it moves to, or that leaves it out of
 * view. The match near the end shows in the expanded viewer, and the bound holds.
 */
export const HugeArrayFindsALateMatch: Story = {
  args: { value: hugeArray },
  play: async (context) => {
    const viewer = await findExpanded(context, 'Provider response', 'item-09876')
    await expect(within(viewer).getByRole('status')).toHaveTextContent(/^1 of 1$/)
    const tree = within(viewer).getByRole('group', { name: 'Structure' })
    await waitFor(() => expect(mounted(tree, /item-\d{5}/)).toBeLessThan(400))
    await waitFor(() => {
      const match = within(viewer).getByRole('mark', { current: true })
      expect(match).toHaveTextContent('item-09876')
      insideOf(match, scrollerOf(tree))
    })
  },
}

/**
 * Catches: a wildcard that first matches across two entries and so never finds the match
 * inside one: `k*v` matches the key "k" with its value "v". Raw's own match starts at
 * "k0" and reaches across both entries, so Find also says that Raw shows it.
 */
export const WildcardFindsTheMatchInOneEntry: Story = {
  args: { value: '{"k0":1,"k":"v"}' },
  play: async (context) => {
    const viewer = await findExpanded(context, 'Provider response', 'k*v')
    await expect(within(viewer).getByRole('status')).toHaveTextContent(/^1 of 1 · 1 more in Raw$/)
    const parts = within(viewer).getAllByRole('mark', { current: true })
    await expect(parts.map((part) => part.textContent)).toEqual(['k', 'v'])
  },
}

/**
 * Catches: a huge text log that mounts every line. You scroll the card to its last line, and
 * Find shows a late line in the expanded viewer, with only lines near the view mounted.
 */
export const HugeTextScrollsAndFinds: Story = {
  args: { label: 'Log message', format: 'text', value: hugeText },
  play: async (context) => {
    const content = within(context.canvasElement).getByRole('group', { name: 'Payload content' })
    await expect(mounted(content, /log line \d+/)).toBeLessThan(400)
    await scrollsTo(content, 'log line 100000')
    await expect(mounted(content, /log line \d+/)).toBeLessThan(400)
    const viewer = await findExpanded(context, 'Log message', '^log line 99999$')
    await expect(within(viewer).getByRole('status')).toHaveTextContent(/^1 of 1$/)
    const lines = within(viewer).getByRole('group', { name: 'Lines' })
    await waitFor(() => expect(mounted(lines, /log line \d+/)).toBeLessThan(400))
    await waitFor(() => {
      const match = within(viewer).getByRole('mark', { current: true })
      expect(match).toHaveTextContent('log line 99999')
      insideOf(match, scrollerOf(lines))
    })
  },
}

/** Catches: huge JSON that shows only in Raw mounting every line. The card scrolls to its end. */
export const HugeRawScrolls: Story = {
  args: { value: hugeMalformed },
  play: async ({ canvasElement }) => {
    const content = within(canvasElement).getByRole('group', { name: 'Payload content' })
    await expect(within(content).getByRole('group', { name: 'Raw' })).toBeInTheDocument()
    await expect(mounted(content, /row \d+/)).toBeLessThan(400)
    await scrollsTo(content, '"row 50000", [')
    await expect(mounted(content, /row \d+/)).toBeLessThan(400)
  },
}

/**
 * Catches: a huge payload on one line, as minified JSON is, mounted whole in Raw. Only the
 * part near the view mounts, and the card scrolls to its end.
 */
export const HugeSingleLineScrollsInRaw: Story = {
  args: { value: hugeArray },
  play: async (context) => {
    const viewer = await openExpanded(context)
    await context.userEvent.click(within(viewer).getByRole('radio', { name: 'Raw' }))
    const raw = within(viewer).getByRole('group', { name: 'Raw' })
    await expect(hugeArray.length).toBeGreaterThan(100_000)
    await expect(mounted(raw, /item-\d{5}/)).toBeLessThan(2_000)
    const scroller = scrollerOf(raw)
    await waitFor(() => {
      scroller.scrollTop = scroller.scrollHeight
      expect(raw).toHaveTextContent('"item-09999"]')
    })
    await expect(mounted(raw, /item-\d{5}/)).toBeLessThan(2_000)
  },
}

/**
 * Catches: a payload of few lines but many characters that mounts whole. Past the character
 * bound, only the part near the view mounts.
 */
export const ManyCharactersInFewLinesStayBounded: Story = {
  args: {
    label: 'Log message',
    format: 'text',
    value: Array.from({ length: 100 }, (_, index) => `${index} ${'x'.repeat(2_000)}`).join('\n'),
  },
  play: async ({ canvasElement, args }) => {
    const lines = within(canvasElement).getByRole('group', { name: 'Lines' })
    await expect(lines.textContent!.length).toBeLessThan(args.value.length / 4)
  },
}

/**
 * Catches: a bound judged by the rows a large payload first shows. Its list of many items,
 * within the character bound, starts folded, and opened, only the items near the view mount.
 */
export const UnfoldingALongListStaysBounded: Story = {
  args: { value: JSON.stringify({ list: JSON.parse(hugeArray).slice(0, 3_000) }) },
  play: async ({ canvasElement, userEvent, args }) => {
    await expect(args.value.length).toBeLessThan(100_000)
    const tree = structureOf(canvasElement)
    await userEvent.click(within(tree).getByRole('button', { name: 'list: 3000 items' }))
    await waitFor(() => expect(mounted(tree, /item-\d{5}/)).toBeGreaterThan(0))
    await expect(mounted(tree, /item-\d{5}/)).toBeLessThan(400)
  },
}

/**
 * Catches: a focused control that unmounts when its row scrolls far out of view, so focus
 * falls out of the viewer. It keeps focus, and Enter still folds its list.
 */
export const KeepsFocusWhileItsRowScrollsAway: Story = {
  args: { value: hugeArray },
  play: async ({ canvasElement, userEvent }) => {
    const content = within(canvasElement).getByRole('group', { name: 'Payload content' })
    const toggle = within(content).getByRole('button', { name: 'Value: 10000 items' })
    toggle.focus()
    await scrollsTo(content, '"item-09999"')
    await expect(toggle.isConnected).toBe(true)
    await expect(toggle).toHaveFocus()
    await userEvent.keyboard('{Enter}')
    await waitFor(() => expect(toggle).toHaveAttribute('aria-expanded', 'false'))
  },
}
