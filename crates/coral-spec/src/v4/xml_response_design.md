# V4 XML Response Normalization Design

## Problem

DSL v4 OpenAPI sources currently assume JSON response bodies. XML-only APIs such
as AWS Query APIs publish OpenAPI documents with `text/xml` responses and return
real XML envelopes such as `DescribeAlarmsResponse`. Coral needs to consume those
responses without requiring endpoint-specific code.

The first-pass XML parser must not be a generic XML-to-JSON guesser. XML has no
native array marker, so generic conversion creates unstable shapes: one
`<member>` element becomes an object, while multiple `<member>` siblings become
an array. V4 should normalize XML according to the OpenAPI response schema and
`xml` metadata so SQL-facing columns see stable JSON-equivalent shapes.

## Current Context

- `crates/coral-spec/src/v4/surfaces/openapi/responses.rs` selects only
  `application/json` success responses and builds `ResponseSpec` with JSON
  defaults.
- `crates/coral-spec/src/v4/surfaces/openapi/schemas.rs` imports OpenAPI schema
  shapes into `IrType` and `IrTypeShape`, but it drops OpenAPI `xml` metadata.
- `crates/coral-spec/src/v4/ir/rest.rs` stores `RestResponseAttachment` with
  status code, media type, and `ResponseSpec`; it has no XML-specific response
  plan.
- `crates/coral-spec/src/common.rs` defines `ResponseBodyFormat` as `json` or
  `json_each_row`.
- `crates/coral-engine/src/backends/http/response.rs` decodes response bytes
  according to `ResponseBodyFormat`. It parses JSON and JSON-each-row only.
- `crates/coral-engine/src/backends/http/fetch.rs` calls `extract_rows` after
  decoding. Row extraction is already format-neutral because it operates on
  `serde_json::Value`.
- `crates/coral-spec/src/v4/projections/derive.rs` generates projection columns
  from the selected row type. Object fields become scalar or JSON columns;
  nested lists remain JSON columns.

Live validation against AWS CloudWatch showed the exact gap. A patched OpenAPI
descriptor could make Coral call AWS and decode XML into JSON blobs, but a
generic conversion produced AWS `member` lists whose shape varied with item
count.

## Technical Plan

Add schema-driven XML response support to v4 in two layers:

1. Import-time response planning in `coral-spec`.
2. Runtime XML normalization in `coral-engine`.

The importer should select XML success responses when JSON is unavailable, set
`ResponseBodyFormat::Xml`, and attach an XML response shape derived from the
OpenAPI response schema. The runtime should parse XML bytes into a small XML DOM
using `quick-xml`, then normalize that DOM through the stored response shape into
`serde_json::Value`.

The normalized JSON value should represent the OpenAPI schema, not raw XML
parser guesses. For example, if the schema says `MetricAlarms` is an array, then
this XML:

```xml
<MetricAlarms>
  <member>
    <AlarmName>x</AlarmName>
  </member>
</MetricAlarms>
```

normalizes to:

```json
{"MetricAlarms":[{"AlarmName":"x"}]}
```

even when only one `<member>` exists.

### Response Shape Model

Extend `ResponseSpec` with an optional XML plan used only when
`format: xml`:

```text
ResponseSpec {
  format: Xml,
  xml: Some(XmlResponseSpec),
  rows_path: ...
}
```

The XML plan should be serializable because v4 materialization writes response
specs into backend-ready projection artifacts.

Suggested shape:

```text
XmlResponseSpec {
  root_name: Option<String>,
  root: XmlValueSpec,
}

XmlValueSpec =
  Scalar { xml_name, scalar_type }
  Object { xml_name, fields }
  List { xml_name, wrapped, item_xml_name, item }
  Json { xml_name }

XmlFieldSpec {
  name,
  xml_name,
  attribute,
  value
}
```

The names in `name` are normalized JSON/schema field names used by existing
projection paths. The names in `xml_name` are wire XML names from OpenAPI
`xml.name` or the schema/property name fallback.

The XML document root is emitted as a JSON object key. `root_name` is both a
matcher and the top-level normalized key. This preserves the existing
`rows_path` model:

```xml
<DescribeAlarmsResponse>
  <DescribeAlarmsResult>
    <MetricAlarms><member><AlarmName>x</AlarmName></member></MetricAlarms>
  </DescribeAlarmsResult>
</DescribeAlarmsResponse>
```

normalizes to:

```json
{
  "DescribeAlarmsResponse": {
    "DescribeAlarmsResult": {
      "MetricAlarms": [{"AlarmName": "x"}]
    }
  }
}
```

For a collection table, `rows_path:
["DescribeAlarmsResponse","DescribeAlarmsResult","MetricAlarms"]` selects the
normalized array. This keeps XML behavior aligned with the current JSON row
extractor instead of adding a second path interpretation model.

The XML response plan is built directly from the resolved OpenAPI response
schema, not by extending `IrType` in the first slice. That keeps OpenAPI XML
wire metadata local to HTTP response decoding while preserving existing
projection type generation. `$ref` is resolved through the existing importer
helper. `allOf` is flattened with the same conservative property merge behavior
as `import_schema`; conflicting properties produce the same diagnostic class and
fall back to JSON for that branch.

### OpenAPI XML Metadata Rules

Use OpenAPI metadata where present:

- `xml.name` changes the wire element or attribute name.
- `xml.attribute: true` reads a value from the current element attributes.
- `xml.wrapped` controls array wrapper behavior.
- Array item names come from `items.xml.name` when present.

Fallbacks must remain schema-driven:

- Array item element name fallback order is:
  1. `items.xml.name`
  2. `xml.name` on the array schema when `xml.wrapped` is explicitly `false`
  3. `member`
  4. the array field/property name

  The `member` fallback is acceptable only because the OpenAPI schema has
  already said the field is an array. It is a generic XML collection convention,
  not endpoint-specific AWS code. Empty array containers normalize to `[]`.
- If no explicit XML root name exists, use the schema name or selected response
  type name when available, then fall back to the first XML document element.
- Namespaces should compare by local name in the first pass. Preserve neither
  namespace prefixes nor namespace URIs in normalized JSON keys unless a later
  API proves that namespace distinction is required.
- Missing elements or attributes normalize as absent fields, not empty strings.
- Attribute fields are scalar values read from the current element's attributes.
  If an attribute field is declared with a non-scalar schema, emit a diagnostic
  at import time and treat that field as JSON/string-compatible for the first
  slice.

Scalar text should be coerced according to schema in the first slice:

- `integer` -> JSON number if parse succeeds, otherwise string
- `number` -> JSON number if parse succeeds, otherwise string
- `boolean` -> JSON boolean for `true`/`false` case-insensitively, otherwise
  string
- `string`, `date-time`, enum, and unknown scalar-compatible values -> string

Failed scalar coercion should not fail the whole request because many XML APIs
are loose. It should preserve the original string.

Media type selection is:

1. Prefer success responses with exact `application/json`.
2. Then JSON structured syntax suffixes such as `application/*+json`.
3. Then exact `text/xml` or `application/xml`.
4. Then XML structured syntax suffixes such as `application/*+xml`.
5. Ignore media type parameters while comparing, so `application/xml; charset=utf-8`
   is XML.

### Runtime Decode Flow

`decode_response_body` should gain an XML branch:

```text
response bytes
  -> quick-xml parser
  -> XmlElement DOM
  -> normalize_with_plan(XmlElement, XmlResponseSpec)
  -> serde_json::Value
  -> existing extract_rows(ResponseSpec, payload)
```

The XML parser should rely on `quick-xml` escaping/unescaping instead of custom
entity-reference code. Unsupported XML features should fail with a decode error
that includes source, table/function, method, and URL, matching JSON decode
errors.

### Projection Behavior

For the first implementation slice, keep projection generation schema-driven:

- Top-level or row-level object fields become columns exactly as they do for
  JSON.
- Schema-normalized arrays become stable JSON columns.
- Existing `rows_path` still controls whether the table returns a singleton,
  list, or wrapped list.
- XML response wrappers that contain one or more non-metadata array payloads
  produce one imported row variant per collection. For AWS-style envelopes this
  turns `DescribeAlarmsResponse -> DescribeAlarmsResult -> MetricAlarms/member`
  into a `metric_alarms` projection with `MetricAlarm` fields as columns, while
  preserving the same HTTP request and full XML response plan.

Response classification stays intentionally conservative. The importer may
identify a single object payload wrapper, then split only direct array fields of
that wrapper into collection variants. It should not attempt arbitrary deep
nested collection table generation. If a schema cannot be classified safely,
expose the normalized XML object as singleton JSON/object columns rather than
inventing rows.

## Alternatives

### Generic XML-to-JSON Only

Rejected. It was enough to prove live AWS calls, but it creates object-or-array
instability for single-element collections and cannot reach JSON parity.

### AWS-Specific Response Helpers

Rejected. AWS Query APIs motivated the work, but Coral should not encode
endpoint-specific wrappers such as `DescribeAlarmsResult` in runtime code.
Wrapper behavior must come from the OpenAPI descriptor and generic XML rules.

### Normalize XML Directly During Streaming Parse

Deferred. A streaming normalizer can be more memory efficient, but a small DOM
first is simpler and matches the existing JSON payload model. Response bodies
already become `serde_json::Value` before row extraction.

### Full Nested Row Projection Generation Now

Deferred. Stable arrays are the prerequisite. Generating nested collection
tables before normalized arrays exist would couple projection behavior to the
wrong parser shape.

## Detailed Implementation

Expected files:

- `crates/coral-spec/src/common.rs`
  Add `ResponseBodyFormat::Xml` and serializable XML response-plan structs under
  `ResponseSpec`.

- `crates/coral-spec/src/v4/surfaces/openapi/schemas.rs`
  Preserve enough OpenAPI XML metadata while importing schema fields, or add
  helper functions that build XML plans from resolved schemas without changing
  the existing `IrType` model more than necessary.

- `crates/coral-spec/src/v4/surfaces/openapi/responses.rs`
  Select `application/json` first, then XML media types such as `text/xml`,
  `application/xml`, and `*/xml`. Build `ResponseSpec { format: Xml, xml:
  Some(...) }` for XML responses. Classify rows using schema names and XML
  wrapper names rather than JSON-only assumptions.

- `crates/coral-spec/src/v4/ir/rest.rs`
  No change expected unless the XML plan is kept in REST-specific IR instead of
  `ResponseSpec`. Prefer `ResponseSpec` so compiled HTTP tables/functions carry
  all runtime decode data.

- `crates/coral-spec/src/v4/openapi_tests.rs`
  Add importer tests for XML-only responses, `xml.name`, attributes, wrapped
  arrays, and AWS-style `member` item fallback.

- `crates/coral-spec/src/v4/projection_tests.rs`
  Add tests proving XML row fields generate stable JSON columns for array
  fields.

- `crates/coral-engine/Cargo.toml`
  Add direct `quick-xml` dependency if not already direct. `Cargo.lock` already
  contains it transitively through `object_store`, but the engine should declare
  what it uses.

- `crates/coral-engine/src/backends/http/response.rs`
  Add the XML decode branch. Keep JSON behavior unchanged.

- `crates/coral-engine/src/backends/http/xml.rs`
  New module for XML parsing and schema normalization. Keep it backend-local
  because it is response-body decode behavior, not source-spec parsing.

- `crates/coral-engine/src/backends/http/mod.rs`
  Export the new `xml` module privately.

- `crates/coral-engine/src/backends/http/fetch.rs` and
  `crates/coral-engine/src/backends/http/transport.rs`
  Thread the XML plan alongside `ResponseBodyFormat` if the existing transport
  only receives the format enum.

- `crates/coral-engine/src/backends/http/*tests*.rs`
  Add decode tests covering single and multiple XML array items, attributes,
  namespaces by local name, and malformed XML errors.

- `crates/coral-spec/src/schema/source_manifest.schema.json` and
  `crates/coral-spec/src/schema/source_manifest_v4.schema.json`
  Regenerate if schema-generation detects changes from the new response fields.

## Acceptance Criteria

- V4 OpenAPI import accepts XML-only success responses and materializes
  `format: xml` plus an XML response plan.
- V3 authored manifests are not expanded for XML in this change.
- JSON and JSON-each-row behavior remains unchanged.
- A schema array normalizes to a JSON array even when the XML body contains one
  item.
- AWS-style collection containers such as `MetricAlarms/member` normalize to a
  stable array when the schema says `MetricAlarms` is an array.
- XML attributes map to fields when `xml.attribute: true` is present.
- Namespaced element names match by local name in the first pass.
- Malformed XML produces a normal HTTP decode error.
- Focused unit tests pass for `coral-spec` and `coral-engine`.
- `make schema-check` passes if response schema files are affected.
- A live CloudWatch query against the patched OpenAPI descriptor returns
  `metric_alarms` rows with alarm fields as columns, including the one-item page
  case that previously collapsed to a JSON object in generic XML parsing.

## Open Questions

- Should deeper nested array row projections be generated automatically after
  direct response collections, or should v4 require an explicit projection rule
  for nested collections?
