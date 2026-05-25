# Open Food Facts

[Open Food Facts](https://world.openfoodfacts.org/) is a free, open, and collaborative database of food products from around the world, containing information on ingredients, allergens, nutrition facts, and eco-scores.

This source provides a SQL interface to the Open Food Facts REST API, allowing you to query food product data, nutritional information, allergens, and taxonomy.

## Setup

No authentication is required to use this source. Simply add the source to your Coral project.

```bash
coral source add openfoodfacts
```

## Tables

The `openfoodfacts` source includes 20 tables tailored to different product attributes and search patterns:

### General Search
*   `product_search`: General search across all products based on tags like categories, brands, and countries.
*   `product_by_barcode`: Retrieve comprehensive data for a specific product by its barcode.

### Nutrition
*   `product_nutrition`: Search for products and retrieve key nutritional values (energy, fat, carbs, sugars, proteins, salt) per 100g.
*   `product_nutrition_by_barcode`: Get detailed nutritional values for a single product by barcode.

### Ingredients & Allergens
*   `product_ingredients`: Search products to view ingredients text, analysis tags, and the total count of food additives.
*   `product_ingredients_by_barcode`: Get ingredient details and additive counts for a single product by barcode.
*   `product_allergens`: Find products and their listed allergen and trace tags.
*   `product_allergens_by_barcode`: Get allergen and trace tags for a single product by barcode.
*   `product_additives`: Search products and get their list of food additives.

### Environment & Packaging
*   `product_ecoscore`: Search products to retrieve their environmental Eco-Score and grade.
*   `product_ecoscore_by_barcode`: Get the Eco-Score for a single product by barcode.
*   `product_packaging`: Search products and retrieve packaging information and recycling tags.
*   `product_packaging_by_barcode`: Get packaging details for a single product by barcode.

### Classification & Marketing
*   `product_brands`: Search products and get brand information.
*   `product_categories`: Search products and get category details and hierarchy.
*   `product_origins`: Search products and get ingredient origin information.
*   `product_countries`: Search products and get a list of countries where they are sold.
*   `product_stores`: Search products and get store listings.

### Media
*   `product_images`: Search products to retrieve URLs for front, nutrition, and ingredient images.
*   `product_images_by_barcode`: Get image URLs for a single product by barcode.

## Example Queries

### Find high-protein pizzas
Search the `pizza` category for products and view their protein and calorie content:

```sql
SELECT
  product_name,
  energy_kcal_100g,
  proteins_100g
FROM openfoodfacts.product_nutrition
WHERE categories_tags = 'pizza'
ORDER BY proteins_100g DESC
LIMIT 5;
```

### Check a product's allergens by barcode
Look up a specific product (e.g., a frozen pizza) using its barcode to see if it contains gluten or milk:

```sql
SELECT
  product_name,
  allergens_tags
FROM openfoodfacts.product_allergens_by_barcode
WHERE code = '4001724039143';
```

### Find Nutella products
Search for products matching the brand tag `nutella`:

```sql
SELECT
  product_name,
  brands
FROM openfoodfacts.product_brands
WHERE brands_tags = 'nutella'
LIMIT 10;
```

### Analyze Eco-Scores in a category
Get the environmental Eco-Score grades for products in a specific category:

```sql
SELECT
  product_name,
  ecoscore_grade,
  ecoscore_score
FROM openfoodfacts.product_ecoscore
WHERE categories_tags = 'pizza'
LIMIT 10;
```

## Usage Notes

*   **Rate Limits**: The Open Food Facts API enforces rate limits (typically 15 requests/minute for reads). Coral sets a custom `User-Agent` to help identify the application, but high-volume queries may still hit these limits.
*   **Tags**: When querying by `categories_tags`, `brands_tags`, or `countries_tags`, use the language prefix if applicable (e.g., `en:pizza` or just `pizza` if the standard tag matches).
*   **Missing Data**: Because Open Food Facts is crowd-sourced, some products may have incomplete or missing data (e.g., `NULL` for `ecoscore_score` or an empty list for `allergens_tags`).
