import { renderToStaticMarkup } from 'react-dom/server'
import { expect, it } from 'vitest'
import { Picker } from './picker'

it('reads the placeholder while no option matches the value', () => {
  const options = [{ value: 'bearer', label: 'Bearer token' }]
  const markup = (value: string) =>
    renderToStaticMarkup(<Picker placeholder="Choose a method" value={value} options={options} />)
  expect(markup('')).toContain('Choose a method')
  expect(markup('bearer')).toContain('Bearer token')
  expect(markup('bearer')).not.toContain('Choose a method')
})
