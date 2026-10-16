-- Each named TypeScript type of a Provider Generation, declared once. An
-- Operation's declaration names these types instead of inlining them, and
-- lists the ones it names directly. An Operation's complete declaration is
-- its own text plus every type it reaches through those references.
--
-- Operations of an earlier Generation inline their types and name none.
CREATE TABLE provider_generation_types (
    provider_generation_id TEXT NOT NULL,
    name TEXT NOT NULL,
    declaration TEXT NOT NULL,
    references_json TEXT NOT NULL,

    CONSTRAINT provider_generation_types_pk
        PRIMARY KEY (provider_generation_id, name),
    CONSTRAINT provider_generation_types_generation_fk
        FOREIGN KEY (provider_generation_id)
        REFERENCES provider_generations(id),
    CONSTRAINT provider_generation_types_valid
        CHECK (
            LENGTH(name) > 0
            AND LENGTH(declaration) > 0
            AND LENGTH(references_json) > 0
        )
);

ALTER TABLE materialized_operations
    ADD COLUMN type_references_json TEXT NOT NULL DEFAULT '[]';
