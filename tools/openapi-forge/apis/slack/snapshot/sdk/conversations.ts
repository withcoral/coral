import type { OptionalArgument } from '../helpers';

import type {
  ChannelID,
  CursorPaginationEnabled,
  LocaleAware,
  OptionalTeamAssignable,
  TargetTeam,
  TimelinePaginationEnabled,
  TokenOverridable,
} from './common';

export interface Channel {
  /** @description ID of conversation. */
  channel: string;
}
export interface Emails {
  /** @description List of emails to receive this invite. Either `emails` or `user_ids` must be provided. */
  emails: string[];
  user_ids?: never;
}
interface IncludeAllMetadata {
  /** @description Return all metadata associated with messages. Defaults to `false`. */
  include_all_metadata?: boolean;
}
export interface InviteID {
  /** @description ID of the invite. */
  invite_id: string;
}
export interface IsPrivate {
  /** @description Whether the channel should be private. */
  is_private?: boolean;
}
interface MessageSpecifier extends Channel {
  /** @description Unique identifier of message. */
  ts: string;
}
interface Message {
  /** @description A message to send to the user who requested the invite. */
  message?: string;
}
export interface UserIDs {
  /** List of user IDs to receive this invite. Either `emails` or `user_ids` must be provided. */
  user_ids: string[];
  emails?: never;
}
export interface Users {
  /** @description A comma separated list of user IDs. Up to 1000 users may be listed. */
  users: string;
}
// https://docs.slack.dev/reference/methods/conversations.acceptSharedInvite
export type ConversationsAcceptSharedInviteArguments = TokenOverridable &
  OptionalTeamAssignable &
  (ChannelID | InviteID) &
  IsPrivate & {
    /**
     * @description Name of the channel. If the channel does not exist already in your workspace,
     * this name is the one that the channel will take.
     */
    channel_name: string;
    /** @description Whether you'd like to use your workspace's free trial to begin using Slack Connect. */
    free_trial_accepted?: boolean;
  };

// https://docs.slack.dev/reference/methods/conversations.approveSharedInvite
export interface ConversationsApproveSharedInviteArguments extends InviteID, TargetTeam, TokenOverridable {}

// https://docs.slack.dev/reference/methods/conversations.archive
export interface ConversationsArchiveArguments extends Channel, TokenOverridable {}

// https://docs.slack.dev/reference/methods/conversations.close
export interface ConversationsCloseArguments extends Channel, TokenOverridable {}

// https://docs.slack.dev/reference/methods/conversations.create
export interface ConversationsCreateArguments extends IsPrivate, TokenOverridable, OptionalTeamAssignable {
  /** @description Name of the public or private channel to create. */
  name: string;
}

// https://docs.slack.dev/reference/methods/conversations.declineSharedInvite
export interface ConversationsDeclineSharedInviteArguments extends InviteID, TargetTeam, TokenOverridable {}

// https://docs.slack.dev/reference/methods/conversations.externalInvitePermissions.set
export interface ConversationsExternalInvitePermissionsSetArguments
  extends Channel,
    Required<TargetTeam>,
    TokenOverridable {
  /** @description The type of action be taken: `upgrade` or `downgrade`. */
  action: 'downgrade' | 'upgrade';
}

// https://docs.slack.dev/reference/methods/conversations.history
export interface ConversationsHistoryArguments
  extends Channel,
    IncludeAllMetadata,
    TokenOverridable,
    CursorPaginationEnabled,
    TimelinePaginationEnabled {}

// https://docs.slack.dev/reference/methods/conversations.info
export interface ConversationsInfoArguments extends Channel, TokenOverridable, LocaleAware {
  /**
   @description Set to `true` to include the member count for the specified conversation. Defaults to `false`.
  */
  include_num_members?: boolean;
}

// https://docs.slack.dev/reference/methods/conversations.invite
export interface ConversationsInviteArguments extends Channel, Users, TokenOverridable {
  /**
   * @description When set to `true` and multiple user IDs are provided, continue inviting the valid ones while
   * disregarding invalid IDs. Defaults to `false`.
   */
  force?: boolean;
}

// https://docs.slack.dev/reference/methods/conversations.inviteShared
export type ConversationsInviteSharedArguments = Channel &
  TokenOverridable &
  (Emails | UserIDs) & {
    /** @description Whether invite is to an external limited member. Defaults to `true`. */
    external_limited?: boolean;
  };

// https://docs.slack.dev/reference/methods/conversations.join
export interface ConversationsJoinArguments extends Channel, TokenOverridable {}

// https://docs.slack.dev/reference/methods/conversations.kick
export interface ConversationsKickArguments extends Channel, TokenOverridable {
  user: string;
}

// https://docs.slack.dev/reference/methods/conversations.leave
export interface ConversationsLeaveArguments extends Channel, TokenOverridable {}

// https://docs.slack.dev/reference/methods/conversations.list
export type ConversationsListArguments = OptionalArgument<
  TokenOverridable &
    CursorPaginationEnabled &
    OptionalTeamAssignable & {
      /** @description Set to `true` to exclude archived channels from the list. Defaults to `false`. */
      exclude_archived?: boolean;
      /**
       * @description Mix and match channel types by providing a comma-separated list of any combination of:
       * `public_channel`, `private_channel`, `mpim` or `im`. Defaults to `public_channel`.
       */
      types?: string;
    }
>;

// https://docs.slack.dev/reference/methods/conversations.listConnectInvites
export type ConversationsListConnectInvitesArguments = OptionalArgument<
  TokenOverridable &
    OptionalTeamAssignable & {
      /** @description Maximum number of invites to return. Defaults to `100`. */
      count?: number;
      /** @description Set to `next_cursor` returned by previous call to list items in subsequent page. */
      cursor?: string;
    }
>;

// https://docs.slack.dev/reference/methods/conversations.mark
export interface ConversationsMarkArguments extends MessageSpecifier, TokenOverridable {}

// https://docs.slack.dev/reference/methods/conversations.members
export interface ConversationsMembersArguments extends Channel, TokenOverridable, CursorPaginationEnabled {}

// https://docs.slack.dev/reference/methods/conversations.open
export type ConversationsOpenArguments = (Channel | Users) &
  TokenOverridable & {
    /**
     * @description Do not create a direct message or multi-person direct message.
     * This is used to see if there is an existing dm or mpdm.
     */
    prevent_creation?: boolean;
    /** @description Indicates you want the full IM channel definition in the response. */
    return_im?: boolean;
  };

// https://docs.slack.dev/reference/methods/conversations.rename
export interface ConversationsRenameArguments extends Channel, TokenOverridable {
  /** @description New name for conversation. */
  name: string;
}

// https://docs.slack.dev/reference/methods/conversations.replies
export interface ConversationsRepliesArguments
  extends MessageSpecifier,
    IncludeAllMetadata,
    TokenOverridable,
    CursorPaginationEnabled,
    TimelinePaginationEnabled {}

// https://docs.slack.dev/reference/methods/conversations.requestSharedInvite.approve
export interface ConversationsRequestSharedInviteApproveArguments
  extends InviteID,
    Partial<ChannelID>,
    TokenOverridable {
  /**
   * @description Whether the invited team will have post-only permissions in the channel.
   * Will override the value on the requested invite.
   */
  is_external_limited?: boolean;
  /** @description Optional additional messaging to attach to the invite approval message. */
  message?: {
    /**
     * @description When `true`, will override the user specified message. Otherwise, `text` will be appended to the
     * user specified message on the invite request.
     */
    is_override: boolean;
    /** @description Text to include along with the email invite. */
    text: string;
  };
}

// https://docs.slack.dev/reference/methods/conversations.requestSharedInvite.deny
export interface ConversationsRequestSharedInviteDenyArguments extends InviteID, Message, TokenOverridable {}

// https://docs.slack.dev/reference/methods/conversations.requestSharedInvite.list
export type ConversationsRequestSharedInviteListArguments = OptionalArgument<
  CursorPaginationEnabled &
    TokenOverridable & {
      /** @description When `true` approved invitation requests will be returned, otherwise they will be excluded. */
      include_approved?: boolean;
      /** @description When `true` denied invitation requests will be returned, otherwise they will be excluded. */
      include_denied?: boolean;
      /** @description When `true` expired invitation requests will be returned, otherwise they will be excluded. */
      include_expired?: boolean;
      /** @description An optional list of invitation ids to look up. */
      invite_ids?: string[];
      /** @description Optional filter to return invitation requests for the inviting user. */
      user_id?: string;
    }
>;

// https://docs.slack.dev/reference/methods/conversations.setPurpose
export interface ConversationsSetPurposeArguments extends Channel, TokenOverridable {
  /** @description A new, specialer purpose. */
  purpose: string;
}

// https://docs.slack.dev/reference/methods/conversations.setTopic
export interface ConversationsSetTopicArguments extends Channel, TokenOverridable {
  /** @description The new topic string. Does not support formatting or linkification. */
  topic: string;
}

// https://docs.slack.dev/reference/methods/conversations.unarchive
export interface ConversationsUnarchiveArguments extends Channel, TokenOverridable {}
