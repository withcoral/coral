---
status: accepted
---

# Return the Program's own failure to the agent

Replacing a thrown error with "Program rejected" left an agent unable to see why its Program failed. A failed Run now
returns the error that actually failed the Program, with its name, message, authored source locations, cause chain
and `AggregateError` members, directly through `exec` and `wait`. An agent can correct its Program without opening
Reef or parsing prose. This reversed an earlier projection that reserved Program stacks for Reef.

The line is the V8 isolate. A diagnostic shows only what the Program itself could see: authored source, never host
stacks, Rust errors, internal causes or credentials. Host-side failures keep their safe public classification. Trusted
Coral fields, such as an Operation Error's code and status, stay distinguishable from Program-controlled text, which a
Program can fill with anything.

The trade-off is that Program-controlled messages can carry upstream data. Access to them follows Run visibility, as
access to the Program's explicit output does, rather than a rule that suppresses thrown messages. Diagnostics stay out
of telemetry, and every part of one is bounded so that a single failure cannot exhaust a response.

## The terminal outcome comes on the last page

Run Updates report the Run's state on every page but carry the terminal outcome, a failure or the Final Value, only on
the last. `has_more` stays true while Events or the outcome remain, even after the state is terminal. Repeating the
outcome on every page could stop an Event that would otherwise fit from being delivered; delivering the outcome once,
after the Events, keeps both complete without trimming either.
