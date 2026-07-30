import type {
  CursorPaginationEnabled,
  FileArgument,
  FileCommentArgument,
  MessageArgument,
  OptionalTeamAssignable,
  TokenOverridable,
  TraditionalPagingEnabled,
} from './common';

/** @description When starring something, it can be starred _to_ a channel. */
export interface StarsChannelDestination {
  /** @description Encoded channel ID the star belongs to. */
  channel: string;
}
// https://docs.slack.dev/reference/methods/stars.add & https://docs.slack.dev/reference/methods/stars.remove
export type StarsAddRemoveArguments = TokenOverridable &
  (StarsChannelDestination | MessageArgument | FileArgument | FileCommentArgument);
// https://docs.slack.dev/reference/methods/stars.list
export interface StarsListArguments
  extends TokenOverridable,
    TraditionalPagingEnabled,
    CursorPaginationEnabled,
    OptionalTeamAssignable {}
