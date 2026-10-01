import { createVar, globalStyle, style } from '@vanilla-extract/css'

import { lightTheme } from '@/wax/theme/theme-light.css'
import { theme } from '@/wax/theme/theme.css'

const commentColor = createVar()
const punctuationColor = createVar()
const propertyColor = createVar()
const selectorColor = createVar()
const operatorColor = createVar()
const attributeColor = createVar()
const variableColor = createVar()
const functionColor = createVar()

export const code = style({
  vars: {
    [attributeColor]: '#569cd6',
    [commentColor]: '#6a9955',
    [functionColor]: '#dcdcaa',
    [operatorColor]: '#CECFD2',
    [propertyColor]: '#b5cea8',
    [punctuationColor]: '#CECFD2',
    [selectorColor]: '#ce9178',
    [variableColor]: '#9cdcfe',
  },
})

export const block = style({
  ...theme.typography.code,
  backgroundColor: theme.surface.main,
  border: `1px solid ${theme.stroke.primary}`,
  borderRadius: 12,
  color: theme.content.primary,
  display: 'block',
  margin: 0,
  overflow: 'auto',
  paddingBlock: 8,
  paddingInlineEnd: 8,
  paddingInlineStart: 16,
  position: 'relative',
  tabSize: 2,
})

function tokens(...names: string[]) {
  return names.map((name) => `${code} .token.${name}`).join(', ')
}

globalStyle(`${lightTheme} ${code}`, {
  vars: {
    [attributeColor]: '#0000FF',
    [commentColor]: '#008000',
    [functionColor]: '#795E26',
    [operatorColor]: theme.content.primary,
    [propertyColor]: '#098658',
    [punctuationColor]: theme.content.primary,
    [selectorColor]: '#A31515',
    [variableColor]: '#001080',
  },
})

globalStyle(tokens('cdata', 'comment', 'doctype', 'prolog'), {
  color: commentColor,
})

globalStyle(tokens('punctuation'), {
  color: punctuationColor,
})

globalStyle(tokens('boolean', 'constant', 'deleted', 'number', 'property', 'symbol', 'tag'), {
  color: propertyColor,
})

globalStyle(tokens('builtin', 'char', 'inserted', 'selector', 'string'), {
  color: selectorColor,
})

globalStyle(tokens('entity', 'operator', 'url'), {
  color: operatorColor,
})

globalStyle(tokens('atrule', 'attr', 'keyword'), {
  color: attributeColor,
})

globalStyle(tokens('important', 'namespace', 'regex', 'variable'), {
  color: variableColor,
})

globalStyle(tokens('class', 'class-name', 'function'), {
  color: functionColor,
})
