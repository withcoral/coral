# Google Fonts Connector

Query the Google Fonts catalog through the Google Fonts Developer API from Coral SQL.

This community source exposes Google Fonts metadata as Coral tables so users can browse the font catalog, inspect individual font families, discover supported scripts, and retrieve optional metadata such as variable font axes, design-space tags, and WOFF2 assets.

**Version:** 0.1.0
**Backend:** HTTP
**Tables:** 2
**Base URL:** `https://www.googleapis.com/webfonts/v1`

## Why this source

Google Fonts is one of the most widely used open font repositories. Designers, developers, and content teams often need to:

* Browse available font families.
* Filter fonts by category.
* Find fonts supporting specific scripts.
* Discover recently added or trending fonts.
* Inspect variable font metadata.
* Retrieve WOFF2 download URLs.

This source exposes that information directly through Coral SQL without requiring users to manually call the Google Fonts API.

The initial release focuses on read-only catalog access and family-level metadata retrieval.

# Authentication

Add the source:

```bash
coral source add --interactive --file sources/community/google_fonts/manifest.yaml
```

When prompted, provide:

```text
GOOGLE_FONTS_API_KEY
```

Paste the API key value directly.

Create an API key:

1. Open https://console.cloud.google.com/apis/credentials
2. Enable the Web Fonts Developer API
3. Create an API key
4. Supply the key during source setup

Verify configuration:

```bash
coral source test google_fonts
```

---

# Tables

## google_fonts.fonts

Returns font families from the Google Fonts catalog.

Maps to:

```http
GET /webfonts
```

### Supported Filters

| Filter     | Description                         |
| ---------- | ----------------------------------- |
| sort       | Sort catalog results                |
| category   | Restrict by font category           |
| subset     | Restrict by supported script subset |
| capability | Request extended metadata           |

### Supported Sort Values

| Value      |
| ---------- |
| alpha      |
| popularity |
| style      |
| date       |
| trending   |

### Supported Categories

| Value       |
| ----------- |
| serif       |
| sans-serif  |
| monospace   |
| display     |
| handwriting |

### Supported Capabilities

| Value       | Description                        |
| ----------- | ---------------------------------- |
| WOFF2       | Return WOFF2 file URLs             |
| VF          | Return variable font axis metadata |
| FAMILY_TAGS | Return design-space tags           |

---

## google_fonts.font

Returns metadata for a single font family.

Maps to:

```http
GET /webfonts?family={family}
```

Required filter:

```sql
family
```

Optional filter:

```sql
capability
```

Returns zero rows when the family does not exist.

---

# Examples

## Browse the catalog

```sql
SELECT family, category
FROM google_fonts.fonts
LIMIT 20;
```

## Most popular families

```sql
SELECT family, category, variants
FROM google_fonts.fonts
WHERE sort = 'popularity'
LIMIT 20;
```

## Alphabetical ordering

```sql
SELECT family, category
FROM google_fonts.fonts
WHERE sort = 'alpha';
```

## Recently added families

```sql
SELECT family, last_modified
FROM google_fonts.fonts
WHERE sort = 'date'
LIMIT 20;
```

## Families with many styles

```sql
SELECT family, variants
FROM google_fonts.fonts
WHERE sort = 'style'
LIMIT 20;
```

## Trending fonts

```sql
SELECT family, category
FROM google_fonts.fonts
WHERE sort = 'trending'
LIMIT 20;
```

---

# Category Filters

## Serif families

```sql
SELECT family, variants
FROM google_fonts.fonts
WHERE category = 'serif'
LIMIT 20;
```

## Sans-serif families

```sql
SELECT family, variants
FROM google_fonts.fonts
WHERE category = 'sans-serif'
LIMIT 20;
```

## Handwriting fonts

```sql
SELECT family, version
FROM google_fonts.fonts
WHERE category = 'handwriting'
LIMIT 20;
```

---

# Subset Filters

## Cyrillic support

```sql
SELECT family, subsets
FROM google_fonts.fonts
WHERE subset = 'cyrillic'
LIMIT 20;
```

## Greek support

```sql
SELECT family, subsets
FROM google_fonts.fonts
WHERE subset = 'greek'
LIMIT 20;
```

## Vietnamese support

```sql
SELECT family, subsets
FROM google_fonts.fonts
WHERE subset = 'vietnamese'
LIMIT 20;
```

---

# Combined Filters

```sql
SELECT family, subsets
FROM google_fonts.fonts
WHERE category = 'sans-serif'
  AND subset = 'cyrillic'
LIMIT 20;
```

---

# Variable Fonts

Request variable font metadata:

```sql
SELECT family, axes
FROM google_fonts.fonts
WHERE capability = 'VF'
LIMIT 20;
```

Inspect a specific family:

```sql
SELECT family, axes
FROM google_fonts.font
WHERE family = 'Inter'
  AND capability = 'VF';
```

Check weight ranges:

```sql
SELECT family, axes
FROM google_fonts.font
WHERE family = 'Roboto Flex'
  AND capability = 'VF';
```

---

# WOFF2 Assets

Retrieve compressed font files:

```sql
SELECT family, files
FROM google_fonts.fonts
WHERE capability = 'WOFF2'
LIMIT 20;
```

Single family:

```sql
SELECT family, files
FROM google_fonts.font
WHERE family = 'Open Sans'
  AND capability = 'WOFF2';
```

---

# Design-Space Tags

```sql
SELECT family, tags
FROM google_fonts.fonts
WHERE capability = 'FAMILY_TAGS'
LIMIT 20;
```

Single family:

```sql
SELECT family, tags
FROM google_fonts.font
WHERE family = 'Noto Sans Display'
  AND capability = 'FAMILY_TAGS';
```

---

# Family Lookup Examples

## Roboto

```sql
SELECT family,
       category,
       variants,
       subsets,
       version,
       last_modified
FROM google_fonts.font
WHERE family = 'Roboto';
```

## Family names containing spaces

```sql
SELECT family, variants
FROM google_fonts.font
WHERE family = 'Noto Sans Display';
```

## Verify existence

```sql
SELECT family
FROM google_fonts.font
WHERE family = 'NonExistentFontXYZ'
LIMIT 1;
```

Expected:

```text
0 rows
```

---

# Debugging

Inspect raw API output:

```sql
SELECT family, raw
FROM google_fonts.font
WHERE family = 'Roboto';
```

Inspect catalog raw output:

```sql
SELECT family, raw
FROM google_fonts.fonts
LIMIT 1;
```

---

# Testing

Run all connector smoke tests:

```bash
coral source test google_fonts
```

Manual verification:

```bash
coral sql "
SELECT family, category
FROM google_fonts.fonts
WHERE sort = 'popularity'
LIMIT 10
"
```

```bash
coral sql "
SELECT family, axes
FROM google_fonts.font
WHERE family = 'Inter'
AND capability = 'VF'
"
```

```bash
coral sql "
SELECT family, files
FROM google_fonts.font
WHERE family = 'Open Sans'
AND capability = 'WOFF2'
"
```

---

# Notes

* Read-only connector.
* No OAuth required.
* Uses API-key authentication only.
* Google Fonts returns the catalog in a single response.
* Family names must exactly match Google Fonts naming.
* Variable font metadata is only returned when `capability = 'VF'`.
* Design-space tags are only returned when `capability = 'FAMILY_TAGS'`.
* WOFF2 URLs are only returned when `capability = 'WOFF2'`.
* Color font metadata is available only for families that support color formats.
* Coral currently supports only a single `capability` filter value at a time. The Google Fonts API accepts repeated `capability` parameters, but Coral source validation does not yet support repeated filters for this source.
