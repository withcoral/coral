# Artifact schema v3 fixtures

These immutable YAML files are known structurally compatible examples of the
artifact schema v3 wire format. Coral must continue to load them through direct
deserialization. They pin compatibility cases such as omitted defaultable
fields and stale producer metadata, and must not be regenerated from the
current Rust models merely to make tests pass.

This is a backward-read guarantee for these exact fixtures, not a migration
layer or a promise that every historical schema v3 artifact is supported. Add
a new versioned fixture directory when introducing a new artifact schema. If
support for one of these fixtures is deliberately withdrawn, update its
consuming compatibility tests explicitly rather than rewriting the fixture.
