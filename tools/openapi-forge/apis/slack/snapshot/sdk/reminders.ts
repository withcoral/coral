import type { OptionalArgument } from '../helpers';
import type { OptionalTeamAssignable, TokenOverridable } from './common';

interface ReminderRecurrenceDailyMonthlyYearly {
  /** @description Specifies the repeating behavior of a reminder. */
  frequency: 'daily' | 'monthly' | 'yearly';
}
type DaysOfTheWeek = 'monday' | 'tuesday' | 'wednesday' | 'thursday' | 'friday' | 'saturday' | 'sunday';
interface ReminderRecurrenceWeekly {
  /** @description Specifies the repeating behavior of a reminder. */
  frequency: 'weekly';
  /** @description Specifies the day-of-the-week repeating behaviour when `frequency` is set to `weekly`. */
  weekdays: [DaysOfTheWeek, ...DaysOfTheWeek[]];
}
type ReminderRecurrence = ReminderRecurrenceWeekly | ReminderRecurrenceDailyMonthlyYearly;
// https://docs.slack.dev/reference/methods/reminders.add
export interface RemindersAddArguments extends TokenOverridable, OptionalTeamAssignable {
  /** @description The content of the reminder. */
  text: string;
  /**
   * @description When this reminder should happen, one of:
   * - the Unix timestamp (up to five years from now),
   * - the number of seconds until the reminder (if within 24 hours), or
   * - a natural language description (Ex. "in 15 minutes," or "every Thursday").
   */
  time: string | number;
  /**
   * @description No longer supported - reminders cannot be set for other users.
   * @deprecated
   * @see {@link https://docs.slack.dev/changelog/2023-07-its-later-already-for-stars-and-reminders Changes to `reminders.*` APIs announcement}.
   */
  user?: string;
  /**
   * @description Specify the repeating behavior of a reminder. If you set the sub-property `frequency` to `weekly`,
   * you must also set the `weekdays` array to specify which days of the week to recur on.
   */
  recurrence?: ReminderRecurrence;
}
// https://docs.slack.dev/reference/methods/reminders.complete
export interface RemindersCompleteArguments extends TokenOverridable, OptionalTeamAssignable {
  /** @description The ID of the reminder to be marked as complete. */
  reminder: string;
}
// https://docs.slack.dev/reference/methods/reminders.delete
export interface RemindersDeleteArguments extends TokenOverridable, OptionalTeamAssignable {
  /** @description The ID of the reminder to delete. */
  reminder: string;
}
// https://docs.slack.dev/reference/methods/reminders.info
export interface RemindersInfoArguments extends TokenOverridable, OptionalTeamAssignable {
  /** @description The ID of the reminder to retrieve information about. */
  reminder: string;
}
// https://docs.slack.dev/reference/methods/reminders.list
export type RemindersListArguments = OptionalArgument<TokenOverridable & OptionalTeamAssignable>;
