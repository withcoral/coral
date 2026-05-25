# Neo4j

Query nodes, relationships, indexes, and constraints from
[Neo4j](https://neo4j.com/) graph databases using SQL via the
[HTTP transactional API](https://neo4j.com/docs/http-api/current/).

## How it works

Neo4j stores data as a property graph — nodes connected by typed relationships.
This source sends Cypher queries to the Neo4j HTTP transactional API
(`/db/<database>/tx/commit`) and maps the results into flat SQL-queryable
tables. The `nodes` table accepts a `label` filter and returns all properties
as a JSON object, making it work with any graph schema. Relationships are
exposed as a single `relationships` table. Schema metadata (indexes,
constraints, labels, relationship types) is exposed as dedicated tables.

## Authentication

Neo4j uses HTTP Basic Auth. Set `NEO4J_USERNAME` and `NEO4J_PASSWORD` to match
your instance credentials. The default credentials for a fresh Docker instance
are `neo4j` / `password` (set via `NEO4J_AUTH=neo4j/password`).

## Local Setup

```bash
docker run -d \
  --name neo4j \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/password \
  neo4j:latest
```

Neo4j Browser will be available at `http://localhost:7474`.
Log in with username `neo4j` and password `password`.

### Tutorial: sample graph

The queries below create a small sample graph you can use to explore the
source. Run them in the Neo4j Browser query editor.

#### 1. Create sample nodes and relationships

```cypher
CREATE
  (u1:User {id: 1, name: 'JARS', email: 'jars@example.com'}),
  (u2:User {id: 2, name: 'Alex', email: 'alex@example.com'}),
  (u3:User {id: 3, name: 'Sarah', email: 'sarah@example.com'}),
  (o1:Organization {id: 101, name: 'OpenAI'}),
  (o2:Organization {id: 102, name: 'NeoTech'}),
  (p1:Product {id: 201, name: 'RTX 5090', category: 'GPU'}),
  (p2:Product {id: 202, name: 'MacBook Pro', category: 'Laptop'}),
  (p3:Product {id: 203, name: 'Quest 4', category: 'VR'}),
  (ord1:Order {id: 301, total: 4500}),
  (ord2:Order {id: 302, total: 2200}),
  (t1:Technology {name: 'AI'}),
  (t2:Technology {name: 'Cloud'}),
  (t3:Technology {name: 'GraphDB'}),
  (u1)-[:WORKS_AT]->(o1),
  (u2)-[:WORKS_AT]->(o2),
  (u3)-[:WORKS_AT]->(o1),
  (u1)-[:INTERESTED_IN]->(t1),
  (u1)-[:INTERESTED_IN]->(t2),
  (u2)-[:INTERESTED_IN]->(t3),
  (u1)-[:PLACED]->(ord1),
  (u2)-[:PLACED]->(ord2),
  (ord1)-[:CONTAINS]->(p1),
  (ord1)-[:CONTAINS]->(p3),
  (ord2)-[:CONTAINS]->(p2),
  (o1)-[:USES]->(t1),
  (o1)-[:USES]->(t2),
  (o2)-[:USES]->(t3);
```

#### 2. Add indexes

```cypher
CREATE INDEX user_email_index FOR (u:User) ON (u.email);
CREATE INDEX product_name_index FOR (p:Product) ON (p.name);
CREATE INDEX organization_name_index FOR (o:Organization) ON (o.name);
```

#### 3. Add constraints

```cypher
CREATE CONSTRAINT user_id_unique IF NOT EXISTS
FOR (u:User)
REQUIRE u.id IS UNIQUE;

CREATE CONSTRAINT product_id_unique IF NOT EXISTS
FOR (p:Product)
REQUIRE p.id IS UNIQUE;

CREATE CONSTRAINT organization_id_unique IF NOT EXISTS
FOR (o:Organization)
REQUIRE o.id IS UNIQUE;
```

#### 4. Verify metadata

```cypher
CALL db.labels();
CALL db.relationshipTypes();
SHOW INDEXES;
SHOW CONSTRAINTS;
```

## Configuration

| Input              | Kind     | Required | Default  | Description                                                        |
|--------------------|----------|----------|----------|--------------------------------------------------------------------|
| `NEO4J_URL`        | variable | no       | `http://localhost:7474` | Base URL of the Neo4j HTTP interface          |
| `NEO4J_USERNAME`   | variable | no       | `neo4j`  | Neo4j username                                                     |
| `NEO4J_PASSWORD`   | secret   | yes      |          | Neo4j password (set via `NEO4J_AUTH=neo4j/<password>` in Docker)   |
| `NEO4J_DATABASE`   | variable | no       | `neo4j`  | Name of the database to query. Change for named databases.         |

## Schema

### `node_labels`

One row per node label defined in the database. Start here to discover what
node types exist before querying the `nodes` table.

### `relationship_types`

One row per relationship type. Use these values to filter the `relationships`
table by `rel_type`.

### `nodes`

Generic node table. Requires a `label` filter. Returns one row per node with:

| Column       | Type  | Description                                      |
|--------------|-------|--------------------------------------------------|
| `element_id` | Utf8  | Neo4j element ID for the node (not guaranteed stable across restores or imports) |
| `labels`     | Json  | All labels on the node as a JSON array           |
| `properties` | Json  | All node properties as a JSON object             |

Works with any graph schema — no assumptions about property names.

### `relationships`

All relationships in the graph. Fields: `from_element_id`, `from_label`,
`rel_type`, `rel_properties`, `to_element_id`, `to_label`. Filter by
`rel_type` in a WHERE clause to scope results.

### `indexes`

All indexes defined in the database. Fields: `name`, `type`, `state`,
`entity_type`, `labels_or_types`, `properties`.

### `constraints`

All constraints defined in the database. Fields: `name`, `type`,
`entity_type`, `labels_or_types`, `properties`.

## Example Queries

```sql
-- Discover all node labels in the graph
SELECT label FROM neo4j.node_labels;

-- Discover all relationship types
SELECT relationship_type FROM neo4j.relationship_types;

-- List all User nodes (returns properties as JSON)
SELECT element_id, properties
FROM neo4j.nodes
WHERE label = 'User';

-- Extract a specific property from nodes
SELECT
  element_id,
  properties->>'name' AS name,
  properties->>'email' AS email
FROM neo4j.nodes
WHERE label = 'User';

-- List all Organization nodes
SELECT element_id, properties
FROM neo4j.nodes
WHERE label = 'Organization';

-- Discover all node labels (use this list to drive per-label queries)
SELECT label FROM neo4j.node_labels;

-- All WORKS_AT relationships
SELECT from_element_id, from_label, to_element_id, to_label
FROM neo4j.relationships
WHERE rel_type = 'WORKS_AT';

-- Count relationships by type
SELECT rel_type, COUNT(*) AS count
FROM neo4j.relationships
GROUP BY rel_type
ORDER BY count DESC;

-- List all ONLINE indexes
SELECT name, type, entity_type, labels_or_types, properties
FROM neo4j.indexes
WHERE state = 'ONLINE';

-- List all uniqueness constraints
SELECT name, labels_or_types, properties
FROM neo4j.constraints
WHERE type = 'NODE_PROPERTY_UNIQUENESS';
```

## Notes

- The `nodes` table requires a `label` filter and works with any graph schema.
  Properties are returned as a JSON object — use `->>'key'` to extract
  individual values in SQL. All matching nodes are fetched from Neo4j before
  SQL filtering applies, so use a selective label to avoid large result sets.
- The `relationships` table fetches all relationships from Neo4j before SQL
  filtering applies. Add a `WHERE rel_type = '...'` clause to reduce the
  result set, but be aware the full graph traversal still runs in Cypher first.
- `from_element_id` and `to_element_id` in `relationships` are Neo4j element
  IDs. They are suitable for joining within a session but are not guaranteed
  stable across database restores, imports, or migrations — use a business
  key property for persistent cross-session references.
- The `labels_or_types` and `properties` columns in `indexes` and `constraints`
  are JSON arrays (e.g. `["User"]`, `["id"]`).
- Set `NEO4J_DATABASE` to target a named database. Defaults to `neo4j`.
