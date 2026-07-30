import type { OptionalArgument } from '../helpers';
import type { OptionalTeamAssignable, TokenOverridable } from './common';

// https://docs.slack.dev/reference/methods/dnd.endDnd
export type DndEndDndArguments = OptionalArgument<TokenOverridable>;
// https://docs.slack.dev/reference/methods/dnd.endSnooze
export type DndEndSnoozeArguments = OptionalArgument<TokenOverridable>;
// https://docs.slack.dev/reference/methods/dnd.info
export type DndInfoArguments = OptionalArgument<
  TokenOverridable &
    OptionalTeamAssignable & {
      /** @description User to fetch status for (defaults to authed user). */
      user?: string;
    }
>;
// https://docs.slack.dev/reference/methods/dnd.setSnooze
export interface DndSetSnoozeArguments extends TokenOverridable {
  /** @description Number of minutes, from now, to snooze until. */
  num_minutes: number;
}
// https://docs.slack.dev/reference/methods/dnd.teamInfo
export interface DndTeamInfoArguments extends TokenOverridable, OptionalTeamAssignable {
  /** @description Comma-separated list of users to fetch Do Not Disturb status for. */
  users: string;
}
