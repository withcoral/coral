import classNames from 'classnames'
import React, { CSSProperties, ElementType } from 'react'

import { TypographyVariant } from './types'
import type { ColorVariants } from './typography.css'
import { typography } from './typography.css'
import * as styles from './typography.css'

type TypographyProps<T extends ElementType = 'span'> = Omit<React.ComponentPropsWithoutRef<T>, 'as'> & {
  as?: T
  size?: CSSProperties['fontSize']
  truncate?: boolean
  variant?: ColorVariants
  weight?: CSSProperties['fontWeight']
}

function createTypographyComponent({
  defaultVariant,
  typographyVariant,
}: {
  defaultVariant?: ColorVariants
  typographyVariant: TypographyVariant
}) {
  return function TypographyComponent<T extends ElementType = 'span'>({
    as,
    children,
    className,
    size,
    style,
    truncate,
    variant,
    weight,
    ...rest
  }: TypographyProps<T>) {
    const Component = as ?? 'span'

    const resolvedColor = variant ?? defaultVariant

    const inlineStyle: CSSProperties = {
      ...style,
      ...(size && { fontSize: size }),
      ...(weight && { fontWeight: weight }),
    }

    return (
      <Component
        className={classNames(
          typography({ color: resolvedColor, variant: typographyVariant }),
          { [styles.truncate]: truncate },
          className
        )}
        style={inlineStyle}
        {...rest}
      >
        {children}
      </Component>
    )
  }
}

export const Typography = {
  Body: createTypographyComponent({ defaultVariant: 'secondary', typographyVariant: TypographyVariant.BODY }),
  BodyLarge: createTypographyComponent({
    defaultVariant: 'secondary',
    typographyVariant: TypographyVariant.BODY_LARGE,
  }),
  BodyLargeStrong: createTypographyComponent({
    defaultVariant: 'primary',
    typographyVariant: TypographyVariant.BODY_LARGE_STRONG,
  }),
  BodySmall: createTypographyComponent({
    defaultVariant: 'secondary',
    typographyVariant: TypographyVariant.BODY_SMALL,
  }),
  BodySmallStrong: createTypographyComponent({
    defaultVariant: 'primary',
    typographyVariant: TypographyVariant.BODY_SMALL_STRONG,
  }),
  BodyStrong: createTypographyComponent({
    defaultVariant: 'secondary',
    typographyVariant: TypographyVariant.BODY_STRONG,
  }),
  ButtonStrong: createTypographyComponent({
    defaultVariant: 'primary',
    typographyVariant: TypographyVariant.BUTTON_STRONG,
  }),
  Code: createTypographyComponent({ defaultVariant: 'code', typographyVariant: TypographyVariant.CODE }),
  CodeInline: createTypographyComponent({ defaultVariant: 'code', typographyVariant: TypographyVariant.CODE_INLINE }),
  CodeInlineStrong: createTypographyComponent({
    defaultVariant: 'code',
    typographyVariant: TypographyVariant.CODE_INLINE_STRONG,
  }),
  CodeLarge: createTypographyComponent({ defaultVariant: 'code', typographyVariant: TypographyVariant.CODE_LARGE }),
  CodeSmallInline: createTypographyComponent({
    defaultVariant: 'code',
    typographyVariant: TypographyVariant.CODE_SMALL_INLINE,
  }),
  CodeSmallInlineStrong: createTypographyComponent({
    defaultVariant: 'code',
    typographyVariant: TypographyVariant.CODE_SMALL_INLINE_STRONG,
  }),
  HeadingLarge: createTypographyComponent({
    defaultVariant: 'primary',
    typographyVariant: TypographyVariant.HEADING_LARGE,
  }),
  HeadingMedium: createTypographyComponent({
    defaultVariant: 'primary',
    typographyVariant: TypographyVariant.HEADING_MEDIUM,
  }),
  HeadingSmall: createTypographyComponent({
    defaultVariant: 'primary',
    typographyVariant: TypographyVariant.HEADING_SMALL,
  }),
  HeadingXLarge: createTypographyComponent({
    defaultVariant: 'primary',
    typographyVariant: TypographyVariant.HEADING_X_LARGE,
  }),
  HeadingXSmall: createTypographyComponent({
    defaultVariant: 'primary',
    typographyVariant: TypographyVariant.HEADING_X_SMALL,
  }),
}
