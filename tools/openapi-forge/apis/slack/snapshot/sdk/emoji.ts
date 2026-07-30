import type { OptionalArgument } from '../helpers';
import type { TokenOverridable } from './common';
// https://docs.slack.dev/reference/methods/emoji.list
export type EmojiListArguments = OptionalArgument<
  TokenOverridable & {
    /** @description Include a list of categories for Unicode emoji and the emoji in each category. */
    include_categories?: boolean;
  }
>;
