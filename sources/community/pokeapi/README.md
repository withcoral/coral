# PokeAPI

Query data about Pokemon, Moves, Types, Abilities, and Species from the free [PokeAPI](https://pokeapi.co).

## Setup

No API key or authentication is needed. Add the source directly:

```bash
coral source add --file sources/community/pokeapi/manifest.yaml
```

## Tables

### `pokemon_list`
List Pokemon names and their detail URLs. Uses offset pagination so you can fetch large lists of Pokemon.
- **Columns**: `name` (Utf8), `url` (Utf8)
- **Example:**
```sql
SELECT name, url 
FROM pokeapi.pokemon_list 
LIMIT 10;
```

### `pokemon`
Fetch a specific Pokemon's detailed data by its `name_or_id` filter (e.g., `pikachu` or `25`).
- **Columns**: `id` (Int64), `name` (Utf8), `base_experience` (Int64), `height` (Int64), `weight` (Int64), `abilities` (Json), `stats` (Json), `types` (Json)
- **Example:**
```sql
SELECT id, name, base_experience, height, weight 
FROM pokeapi.pokemon 
WHERE name_or_id = 'pikachu';
```

### `moves`
Fetch a specific Move's detailed data by its `name_or_id` filter.
- **Columns**: `id` (Int64), `name` (Utf8), `accuracy` (Int64), `power` (Int64), `pp` (Int64), `type_name` (Utf8)
- **Example:**
```sql
SELECT id, name, accuracy, power, pp, type_name 
FROM pokeapi.moves 
WHERE name_or_id = 'swords-dance';
```

### `types`
Fetch a specific Type's detailed data by its `name_or_id` filter.
- **Columns**: `id` (Int64), `name` (Utf8), `damage_relations` (Json)
- **Example:**
```sql
SELECT id, name, damage_relations 
FROM pokeapi.types 
WHERE name_or_id = 'fire';
```

### `abilities`
Fetch a specific Ability's detailed data by its `name_or_id` filter.
- **Columns**: `id` (Int64), `name` (Utf8), `effect_entries` (Json)
- **Example:**
```sql
SELECT id, name, effect_entries 
FROM pokeapi.abilities 
WHERE name_or_id = 'overgrow';
```

### `species`
Fetch a specific Pokemon Species' detailed data by its `name_or_id` filter.
- **Columns**: `id` (Int64), `name` (Utf8), `base_happiness` (Int64), `capture_rate` (Int64), `flavor_text_entries` (Json)
- **Example:**
```sql
SELECT id, name, base_happiness, capture_rate 
FROM pokeapi.species 
WHERE name_or_id = 'bulbasaur';
```

## Limits

- This API is entirely free and public.
- **Rate Limiting**: PokeAPI enforces a rate limit of **100 requests per IP address per minute**. This has been documented, and if requests exceed the quota, Coral will seamlessly handle the standard `429 Too Many Requests` response by applying a backoff strategy.
