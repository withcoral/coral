import type { MessageArgument, TokenOverridable } from './common';

// https://docs.slack.dev/reference/methods/pins.add
export interface PinsAddArguments extends MessageArgument, TokenOverridable {}
// https://docs.slack.dev/reference/methods/pins.list
export interface PinsListArguments extends TokenOverridable {
  /** @description Channel to get pinned items for. */
  channel: string;
}
// https://docs.slack.dev/reference/methods/pins.remove
export interface PinsRemoveArguments extends MessageArgument, TokenOverridable {}
