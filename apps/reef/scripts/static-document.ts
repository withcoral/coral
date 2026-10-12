import { createHash } from 'node:crypto'

export interface StaticDocument {
  html: string
  css: string
  stylesheet: string
}

/** Move trusted SSR styles into a content-addressed stylesheet, keeping a
 * script-free document compatible with style-src 'self' (no unsafe-inline).
 * Run only on our React-rendered markup, never on evidence/source strings.
 */
export function staticDocument(markup: string, baseCss: string): StaticDocument {
  const rules = new Set<string>()
  let html = markup.replace(/<style\b[^>]*>([\s\S]*?)<\/style>/g, (_tag, css: string) => {
    rules.add(css)
    return ''
  })
  html = html.replace(/<[a-zA-Z][^>]*>/g, (tag) => {
    const attribute = /\sstyle="([^"]*)"/.exec(tag)
    if (!attribute) return tag
    const css = attribute[1].replace(
      /&(?:quot|amp|lt|gt|#x27);/g,
      (entity) =>
        ({ '&quot;': '"', '&amp;': '&', '&lt;': '<', '&gt;': '>', '&#x27;': "'" })[entity]!,
    )
    const name = `static-${createHash('sha256').update(css).digest('hex').slice(0, 20)}`
    rules.add(`.${name}{${css}}`)
    const stripped = tag.replace(attribute[0], '')
    return /\sclass="/.test(stripped)
      ? stripped.replace(/\sclass="/, ` class="${name} `)
      : stripped.replace(/^<([\w-]+)/, `<$1 class="${name}"`)
  })
  const css = `${baseCss}\n${[...rules].toSorted().join('\n')}`
  const stylesheet = `_wax-${createHash('sha256').update(css).digest('hex')}.css`
  html = html.replace('</head>', `<link rel="stylesheet" href="${stylesheet}"/></head>`)
  return { html, css, stylesheet }
}
