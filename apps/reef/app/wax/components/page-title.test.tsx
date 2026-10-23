import { renderToStaticMarkup } from 'react-dom/server'
import { expect, it } from 'vitest'
import { PageTitle, PageTitleRoot } from './page-title'

it('ends the title with the root name, when there is one', () => {
  const title = <PageTitle>Activity · Demo</PageTitle>
  expect(renderToStaticMarkup(title)).toBe('<title>Activity · Demo</title>')
  expect(renderToStaticMarkup(<PageTitleRoot value="Coral">{title}</PageTitleRoot>)).toBe(
    '<title>Activity · Demo · Coral</title>',
  )
})
