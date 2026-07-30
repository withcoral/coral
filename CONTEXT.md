# Coral

Coral exposes connected data to agents and attributes their work without
conflating current product concepts with retired experimental terminology.

## Language

**Workspace**:
The confidentiality boundary for Coral data and agent work. Tasks are shared by
Principals within a Workspace and are never shared across Workspaces.

**Principal**:
An authenticated actor, stably classified as a User or Agent, whose participation
in a Task is attributed. Kind informs authorization but does not itself grant
permission; a Principal does not privately own a Task.

**Task**:
A Workspace-shared unit of agent work with a declared intent and a terminal
success or failure outcome. It records the Principal that started it, and Coral
interactions within it are attributed to its task identifier.

_Avoid_: Using Episode or session as aliases for Task

**Episode**:
A retired predecessor to Task that is not part of Coral's current product
model.

_Avoid_: Using episode as an alias for Task
