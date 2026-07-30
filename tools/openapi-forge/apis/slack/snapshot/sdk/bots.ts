import type { OptionalArgument } from '../helpers';

import type { OptionalTeamAssignable, TokenOverridable } from './common';

// https://docs.slack.dev/reference/methods/bots.info
export type BotsInfoArguments = OptionalArgument<
  TokenOverridable &
    OptionalTeamAssignable & {
      /** @description Bot ID, which starts with 'B', to retrieve information about. */
      bot?: string;
    }
>;
