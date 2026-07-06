# Dog CEO

Fetch random dog images and filter by breed using the [Dog CEO API](https://dog.ceo/dog-api/).

## Setup

No authentication is required. Add the source:

```bash
coral source add --file sources/community/dog-ceo/manifest.yaml
```

## Local Testing

```bash
coral sql "
  SELECT image_url 
  FROM dog_ceo.breed_images 
  WHERE breed = 'hound' AND count = 2 
  LIMIT 2
"

/*
+---------------------------------------------------------------+
| image_url                                                     |
+---------------------------------------------------------------+
| https://images.dog.ceo/breeds/hound-blood/n02088466_10408.jpg |
| https://images.dog.ceo/breeds/hound-ibizan/n02091244_1340.jpg |
+---------------------------------------------------------------+
*/
```

## Tables

| Table | Description |
|-------|-------------|
| `random_image` | Get a single random dog image. |
| `random_images` | Get an array of random dog images. Requires the `count` filter. |
| `breed_images` | Get an array of random images for a specific breed. Requires `breed` and `count` filters. |
