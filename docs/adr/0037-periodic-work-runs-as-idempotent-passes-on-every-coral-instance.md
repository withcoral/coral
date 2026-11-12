---
status: accepted
---

# Periodic work runs as idempotent passes on every Coral Instance

Coral has work that the clock starts, not a request: the expired-lease sweep, device authorization recovery, the release
check and the Usage Report check. The History Expiry pass
([#2430](https://github.com/withcoral/coral/issues/2430)) is the next. Each is a pass: one function that its owning
module exposes. Bootstrap runs every pass on a timer in every Coral Instance. Coral keeps no table that schedules
passes, and no leader or deployment-wide lock decides which Coral Instance runs them.

A pass is therefore safe to run at the same moment in two Coral Instances. The rows the pass works on settle each unit
of work: the Usage Report check moves the next report day with a compare-and-set, and the History Expiry pass is to
hold the Task row and check again before it deletes. The Coral Instance that comes second finds the work done and moves
on.

The cost is one candidate read for each Coral Instance on each tick, and a short lock wait when two of them reach the
same unit. A pass can delay its first run by an amount that differs between Coral Instances, so that Coral Instances
that started together do not keep meeting.

A pass that can find more work than it should do at once stops at a fixed bound and asks to run again soon. The History
Expiry pass needs this: its first run after an upgrade can find months of history, and on SQLite every write
transaction takes the one write lock.

Two passes keep their own rule. Search reconciliation keeps its own supervisor: it runs two tasks that work together
and already restarts them. The Run sweeper keeps the rule that
[ADR 0027](0027-run-executors-compile-and-execute-programs.md) gives it, one sweeper at a time, elected by a lease row
of its own. Until that is built, the expired-lease sweep runs in every Coral Instance like the other passes. This ADR
does not change ADR 0027.

Rejected: one table of passes with due times and leases. It would decide which Coral Instance runs each pass, and so
save the repeated candidate reads. It needs its own lease, expiry and takeover rules, a second copy of what the Run
worker lease already does, and every pass must tolerate a concurrent run in any case, because a lease can lapse under a
slow holder. A table that one workflow owns, such as the device authorization jobs, is not this: its rows are the work,
not a schedule.

Rejected: a Postgres advisory lock. SQLite has none, and Coral keeps one behavior on both databases.
