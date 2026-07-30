import type { OptionalTeamAssignable, SortDir, TokenOverridable, TraditionalPagingEnabled } from './common';

interface Searchable extends OptionalTeamAssignable, SortDir {
  /** @description Search query. */
  query: string;
  /**
   * @description Set to `true` to enable query highlight markers. Defaults to `false`.
   * @see {@link https://docs.slack.dev/reference/methods/search.messages `search.messages` Usage info} for details.
   */
  highlight?: boolean;
  /** @description Return matches sorted by either `score` or `timestamp`. Defaults to `score`. */
  sort?: 'score' | 'timestamp';
}

interface SearchMessagesCursorPagination {
  /**
   * @description Paginate through collections of data by setting the `cursor` parameter to `*` for the first "page"
   * or a `next_cursor` attribute returned by a previous request's `response_metadata`.
   * Use the `count` parameter to set the number of items to return per page rather than `limit`.
   * @see {@link https://docs.slack.dev/apis/web-api/pagination pagination} for more detail.
   */
  cursor?: string;
}

// https://docs.slack.dev/reference/methods/search.all
export interface SearchAllArguments extends TokenOverridable, TraditionalPagingEnabled, Searchable {}
// https://docs.slack.dev/reference/methods/search.files
export interface SearchFilesArguments extends TokenOverridable, TraditionalPagingEnabled, Searchable {}
// https://docs.slack.dev/reference/methods/search.messages
export interface SearchMessagesArguments
  extends TokenOverridable,
    TraditionalPagingEnabled,
    Searchable,
    SearchMessagesCursorPagination {}
