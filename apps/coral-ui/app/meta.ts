import type { MetaDescriptor } from 'react-router'

const APP_TITLE = 'Coral'

export function pageTitle(title: string): string {
  return `${APP_TITLE} - ${title}`
}

export function getMeta(title: string): MetaDescriptor[] {
  return [{ title: pageTitle(title) }]
}
