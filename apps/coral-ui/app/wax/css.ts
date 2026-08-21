import {
  globalStyle as vanillaGlobalStyle,
  style as vanillaStyle,
  styleVariants as vanillaStyleVariants,
} from '@vanilla-extract/css'
import type { ComplexStyleRule, GlobalStyleRule, StyleRule } from '@vanilla-extract/css'
import { recipe as vanillaRecipe } from '@vanilla-extract/recipes'
import type { RuntimeFn } from '@vanilla-extract/recipes'

import { resetLayer, waxLayer } from '@/wax/theme/layers.css'

/**
 * Instead of relying on bundlers, we want our CSS to follow a specific order:
 * - Reset styles
 * - WAX styles
 * - Everything else (e.g. components)
 *
 * To achieve that, we use `@layer`: one for reset and one for WAX. Everything that's
 * outside a layer will take precedence (given it has the same specificity). That's the C in CSS :P
 */
export * from '@vanilla-extract/css'
export type { RecipeVariants, RuntimeFn } from '@vanilla-extract/recipes'

type VariantGroups = NonNullable<Parameters<typeof vanillaRecipe>[0]['variants']>
type RecipeOptions<Variants extends VariantGroups> = Parameters<typeof vanillaRecipe<Variants>>[0]

function inWaxLayer(rule: StyleRule): StyleRule {
  return { '@layer': { [waxLayer]: rule } }
}

export function globalStyle(selector: string, rule: GlobalStyleRule): void {
  vanillaGlobalStyle(selector, { '@layer': { [waxLayer]: rule } })
}

/** `globalStyle` for an element or pseudo element default. See layers.css.ts. */
export function resetStyle(selector: string, rule: GlobalStyleRule): void {
  vanillaGlobalStyle(selector, { '@layer': { [resetLayer]: rule } })
}

/**
 * `inWaxLayer` for anything the style APIs accept. Class names in a composition
 * pass through: they name rules that already sit in the layer they belong to.
 */
function inWaxLayerRule<Rule extends ComplexStyleRule | string>(rule: Rule): Rule {
  if (typeof rule === 'string') return rule
  if (Array.isArray(rule)) {
    return rule.map((entry) =>
      typeof entry === 'string' || Array.isArray(entry) ? entry : inWaxLayer(entry),
    ) as Rule
  }
  return inWaxLayer(rule) as Rule
}

export function style(rule: ComplexStyleRule, debugId?: string): string {
  return vanillaStyle(inWaxLayerRule(rule), debugId)
}

export function styleVariants<StyleMap extends Record<number | string, ComplexStyleRule>>(
  styleMap: StyleMap,
  debugId?: string,
): Record<keyof StyleMap, string>
export function styleVariants<
  Data extends Record<number | string, unknown>,
  Key extends keyof Data,
>(
  data: Data,
  mapData: (value: Data[Key], key: Key) => ComplexStyleRule,
  debugId?: string,
): Record<keyof Data, string>
export function styleVariants(
  data: Record<number | string, unknown>,
  mapDataOrDebugId?: ((value: unknown, key: unknown) => ComplexStyleRule) | string,
  debugId?: string,
): Record<number | string, string> {
  const mapData = typeof mapDataOrDebugId === 'function' ? mapDataOrDebugId : undefined

  return vanillaStyleVariants(
    data,
    (value, key) => inWaxLayerRule(mapData ? mapData(value, key) : (value as ComplexStyleRule)),
    typeof mapDataOrDebugId === 'string' ? mapDataOrDebugId : debugId,
  )
}

export function recipe<Variants extends VariantGroups>(
  options: RecipeOptions<Variants>,
  debugId?: string,
): RuntimeFn<Variants> {
  const { base, compoundVariants, variants } = options

  return vanillaRecipe<Variants>(
    {
      ...options,
      base: base === undefined ? undefined : inWaxLayerRule(base),
      compoundVariants: compoundVariants?.map((compound) => ({
        ...compound,
        style: inWaxLayerRule(compound.style),
      })),
      variants:
        variants === undefined
          ? undefined
          : (Object.fromEntries(
              Object.entries(variants).map(([group, values]) => [
                group,
                Object.fromEntries(
                  Object.entries(values).map(([value, rule]) => [value, inWaxLayerRule(rule)]),
                ),
              ]),
            ) as Variants),
    },
    debugId,
  )
}
