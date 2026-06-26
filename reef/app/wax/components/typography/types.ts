export const TypographyVariant = {
  BODY: 'body',
  BODY_LARGE: 'bodyLarge',
  BODY_LARGE_STRONG: 'bodyLargeStrong',
  BODY_SMALL: 'bodySmall',
  BODY_SMALL_STRONG: 'bodySmallStrong',
  BODY_STRONG: 'bodyStrong',
  BUTTON_STRONG: 'buttonStrong',
  CODE: 'code',
  CODE_INLINE: 'codeInline',
  CODE_INLINE_STRONG: 'codeInlineStrong',
  CODE_LARGE: 'codeLarge',
  CODE_SMALL_INLINE: 'codeSmallInline',
  CODE_SMALL_INLINE_STRONG: 'codeSmallInlineStrong',
  HEADING_LARGE: 'headingLarge',
  HEADING_MEDIUM: 'headingMedium',
  HEADING_SMALL: 'headingSmall',
  HEADING_X_LARGE: 'headingXLarge',
  HEADING_X_SMALL: 'headingXSmall',
} as const

export type TypographyVariant = (typeof TypographyVariant)[keyof typeof TypographyVariant]
