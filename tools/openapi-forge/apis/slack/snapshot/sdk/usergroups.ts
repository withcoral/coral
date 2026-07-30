import type { OptionalArgument } from '../helpers';
import type { OptionalTeamAssignable, TokenOverridable } from './common';

export interface UsergroupsIncludeCount {
  /** @description Include the number of users in each User Group. */
  include_count?: boolean;
}

// https://docs.slack.dev/reference/methods/usergroups.create
export interface UsergroupsCreateArguments extends TokenOverridable, OptionalTeamAssignable, UsergroupsIncludeCount {
  /** @description A name for the User Group. Must be unique among User Groups. */
  name: string;
  /** @description A comma separated string of encoded channel IDs for which the User Group uses as a default. */
  channels?: string;
  /** @description A short description of the User Group. */
  description?: string;
  /** @description A mention handle. Must be unique among channels, users and User Groups. */
  handle?: string;
}
// https://docs.slack.dev/reference/methods/usergroups.disable
export interface UsergroupsDisableArguments extends TokenOverridable, OptionalTeamAssignable, UsergroupsIncludeCount {
  /** @description The encoded ID of the User Group to disable. */
  usergroup: string;
}
// https://docs.slack.dev/reference/methods/usergroups.enable
export interface UsergroupsEnableArguments extends TokenOverridable, OptionalTeamAssignable, UsergroupsIncludeCount {
  /** @description The encoded ID of the User Group to enable. */
  usergroup: string;
}
// https://docs.slack.dev/reference/methods/usergroups.list
export type UsergroupsListArguments = OptionalArgument<
  TokenOverridable &
    OptionalTeamAssignable &
    UsergroupsIncludeCount & {
      /** @description Include disabled User Groups. */
      include_disabled?: boolean;
      /** @description Include the list of users for each User Group. */
      include_users?: boolean;
    }
>;

// https://docs.slack.dev/reference/methods/usergroups.update
export interface UsergroupsUpdateArguments
  extends TokenOverridable,
    OptionalTeamAssignable,
    Partial<UsergroupsCreateArguments> {
  /** @description The encoded ID of the User Group to update. */
  usergroup: string;
}
// https://docs.slack.dev/reference/methods/usergroups.users.list
export interface UsergroupsUsersListArguments extends TokenOverridable, OptionalTeamAssignable {
  /** @description The encoded ID of the User Group to list users for. */
  usergroup: string;
  /** @description Allow results that involve disabled User Groups. */
  include_disabled?: boolean;
}
// https://docs.slack.dev/reference/methods/usergroups.users.update
export interface UsergroupsUsersUpdateArguments
  extends TokenOverridable,
    OptionalTeamAssignable,
    UsergroupsIncludeCount {
  /** @description The encoded ID of the User Group to update users for. */
  usergroup: string;
  /**
   * @description A comma separated string of encoded user IDs that represent the entire list of users for
   * the User Group.
   */
  users: string;
}
