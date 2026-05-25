# Open Food Facts

[Open Food Facts](https://world.openfoodfacts.org/) is a free, open, and collaborative database of food products from around the world, containing information on ingredients, allergens, nutrition facts, and eco-scores.

This source provides a SQL interface to the Open Food Facts REST API, allowing you to query food product data, nutritional information, allergens, and taxonomy.

## Setup

No authentication is required to use this source. Simply add the source to your Coral project.

```bash
coral source add openfoodfacts
```

## Tables

The `openfoodfacts` source includes 50 tables tailored to different product attributes and search patterns:

### General Search
*   `product_search`, `product_by_barcode`: General product attributes like name, brand, nutriscore, and nova group.
*   `product_misc`, `product_misc_by_barcode`: Miscellaneous fields like PNNS groups and creator.

### Nutrition & Diet
*   `product_nutrition`, `product_nutrition_by_barcode`: Core nutritional values (energy, fat, carbs, sugars, proteins, salt) per 100g.
*   `product_vitamins`, `product_vitamins_by_barcode`: Vitamin content per 100g.
*   `product_minerals`, `product_minerals_by_barcode`: Mineral content per 100g.
*   `product_nutrient_levels`, `product_nutrient_levels_by_barcode`: Nutrient levels categorization.

### Ingredients & Allergens
*   `product_ingredients`, `product_ingredients_by_barcode`: Ingredients text, analysis, and additive counts.
*   `product_allergens`, `product_allergens_by_barcode`: Listed allergens and trace tags.
*   `product_traces`, `product_traces_by_barcode`: Specific trace hierarchies.
*   `product_additives`, `product_additives_by_barcode`: List of food additives.

### Environment & Packaging
*   `product_ecoscore`, `product_ecoscore_by_barcode`: Environmental Eco-Score and grade.
*   `product_packaging`, `product_packaging_by_barcode`: Packaging information and recycling tags.
*   `product_emb_codes`, `product_emb_codes_by_barcode`: Traceability and packer codes.

### Classification & Marketing
*   `product_brands`, `product_brands_by_barcode`: Brand information.
*   `product_categories`, `product_categories_by_barcode`: Category details and hierarchy.
*   `product_origins`, `product_origins_by_barcode`: Ingredient origin information.
*   `product_countries`, `product_countries_by_barcode`: Countries where the product is sold.
*   `product_stores`, `product_stores_by_barcode`: Store listings.
*   `product_labels`, `product_labels_by_barcode`: Marketing and quality labels (e.g. Organic, Vegan).
*   `product_manufacturing`, `product_manufacturing_by_barcode`: Manufacturing places.
*   `product_purchase`, `product_purchase_by_barcode`: Places where the product was purchased.

### Metadata & Media
*   `product_images`, `product_images_by_barcode`: URLs for front, nutrition, and ingredient images.
*   `product_states`, `product_states_by_barcode`: Product completion states.
*   `product_languages`, `product_languages_by_barcode`: Languages used on the packaging.
*   `product_nova`, `product_nova_by_barcode`: NOVA food processing classifications.

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
