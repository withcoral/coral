---
status: accepted
---

# Return the Program's existing failure through MCP

Replacing a thrown error with “Program rejected” prevents an agent from understanding why its Program failed.
Return the error that already caused the Program to fail through the existing Run and MCP surfaces. This is error
propagation, not a new diagnostic execution phase, additional instrumentation, or host investigation. Code already
executing as part of the Program remains ordinary Program execution.

Every failed Run should expose the available permitted error information so the agent can understand the cause and
determine the appropriate correction. Rich runtime diagnostics
must stay within what is visible inside the V8 isolate. Never expose unhandled Rust errors, host stacks, or internal
causes. Host-side failures retain safe public classifications rather than acquiring private error details. Operation
Errors retain their existing defined shape; this change does not expand the Operation error contract. ADR 0019
replaces that sentence: the Run diagnostic of an Operation Error that carries an upstream response exposes its HTTP
status, when it has one, and the first 1 KiB of its encoded data.

Authorized Run readers may receive bounded Program-controlled error messages, authored source locations, and a bounded
chain of causes. Include bounded `AggregateError` members separately from its cause chain. Cycles and truncation must
be handled explicitly, including omitted members. Arbitrary attached object properties are excluded by default.
Trusted Coral error fields remain distinguishable from Program-controlled text. Diagnostic content stays
out of telemetry, host internals and credentials remain private, and no diagnostic promises that repeating a Run is safe.

Expose structured error kinds, messages, authored source locations, and existing Operation error fields directly through
`exec` and `wait`, so an agent need not open Reef or parse prose to investigate a failure. This revises an earlier
MCP projection that reserved detailed Program stacks for Reef. Diagnostics still name only authored source.

The trade-off is that Program-controlled messages may retain upstream data, as explicit Program output can. Access to
that content follows Run visibility rather than a blanket rule suppressing thrown messages. This does not grant agents
new access to host information or other Principals' Runs.

## Representation and bounds

The Run failure keeps its safe `code` and compact `message`. Its optional `error` carries the existing error's
Program-controlled `name` and `message`, authored `frames`, optional `cause`, and `errors` for aggregate members.
The readable summary escapes NUL characters for portable text storage; the JSON error retains them.
Only host-issued Operation fields populate `operation`; assigning similar properties to a Program error does not make
them trusted. A `truncated` flag reports bounded content, `cause_omitted` and `omitted_errors` identify omitted
relationships, and `circular` marks a reference to an ancestor.

Every part is bounded: name and message length, frames per node, nesting depth, total nodes, aggregate members, and
the encoded size as a share of the Run's MCP response limit. The limits are constants in the code and not part of
this decision. Accessor and proxy properties are skipped; formatting uses data properties and V8's existing native exception frames without running custom stack hooks.

## Terminal outcome pagination

Run Updates report the current Run state on every page, but include the terminal outcome only on the final page.
`has_more` means that Events or terminal details remain. Clients continue with the returned cursor even when the
state is already terminal. If the complete outcome cannot fit alongside the remaining Events, those Events are
delivered first with `has_more: true`; the next observation returns the outcome on its own. An observation after
the completed cursor returns the retained outcome again, preserving later inspection without repeating it on
every historical Event page.

This replaces the earlier choice to include terminal data ahead of Event pagination. Repeating the outcome can
prevent an otherwise deliverable Event from fitting the response budget. Separate delivery preserves complete
Events and the same retained diagnostics without response-specific trimming. Each Event and the terminal outcome
must still fit a response individually. The same rule applies to a successful Run's Final Value.
