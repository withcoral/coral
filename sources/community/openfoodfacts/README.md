# Open Food Facts community source

The `openfoodfacts` community source exposes global food product data, including ingredients, allergens, nutrition facts (Nutri-Score, Eco-Score), and brand information from the [Open Food Facts API](https://world.openfoodfacts.org/data) through Coral SQL.

## Setup

The Open Food Facts API is public and does not require an API token for read access! You can install the source immediately:

```sh
cargo run -p coral-cli -- source add --file sources/community/openfoodfacts/manifest.yaml
```

## Tables

| Table | Purpose |
| --- | --- |
| `openfoodfacts.product_search` | Search for food products globally using tags like categories, brands, or countries. |
| `openfoodfacts.product_by_barcode` | Look up a specific food product using its barcode (EAN/UPC) (requires `code` filter). |
| `openfoodfacts.product_nutrition` | Retrieve detailed nutritional information per 100g for products (requires `categories_tags` filter). |

All tables are read-only. 

## Example queries

Search for pizzas and get their Nutri-Score:

```sql
SELECT code, product_name, brands, nutriscore_grade 
FROM openfoodfacts.product_search 
WHERE categories_tags = 'pizza'
LIMIT 10;
```

Look up a specific product by its barcode:

```sql
SELECT product_name, brands, ingredients_text, nova_group 
FROM openfoodfacts.product_by_barcode 
WHERE code = '3017620422003';
```

Get detailed nutritional info for chocolate products:

```sql
SELECT product_name, nutriments__energy_kcal_100g, nutriments__sugars_100g, nutriments__fat_100g 
FROM openfoodfacts.product_nutrition 
WHERE categories_tags = 'chocolate'
LIMIT 10;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/openfoodfacts/manifest.yaml
```

Install and test:

```sh
cargo run -p coral-cli -- source add --file sources/community/openfoodfacts/manifest.yaml
cargo run -p coral-cli -- source test openfoodfacts
```
