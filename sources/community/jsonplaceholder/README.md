# JSONPlaceholder

Query fake JSON data from [JSONPlaceholder](https://jsonplaceholder.typicode.com/). This source is useful for testing, prototyping, and demonstrating SQL capabilities with a simple REST API.

```bash
coral source add --file sources/community/jsonplaceholder/manifest.yaml
```

## Tables

| Table      | Description                                                                 |
| ---------- | --------------------------------------------------------------------------- |
| `users`    | Retrieve a list of 10 fake users.                                           |
| `posts`    | Retrieve 100 fake posts. Can be filtered by `user_id_filter`.               |
| `comments` | Retrieve fake comments. Can be filtered by `post_id_filter`.                |
| `todos`    | Retrieve 200 fake todos. Can be filtered by `user_id_filter`.               |

## Filters

| Filter             | Required | Description                                     |
| ------------------ | -------- | ----------------------------------------------- |
| `user_id_filter`   | No       | Filter posts or todos by a specific `userId`. Note: this is an echoed filter column, not a provider field. |
| `post_id_filter`   | No       | Filter comments by a specific `postId`. Note: this is an echoed filter column, not a provider field. |

## Example queries

```sql
-- Retrieve the first 5 users
SELECT id, name, username, email 
FROM jsonplaceholder.users 
LIMIT 5;

/*
+----+------------------+-----------+---------------------------+
| id | name             | username  | email                     |
+----+------------------+-----------+---------------------------+
| 1  | Leanne Graham    | Bret      | Sincere@april.biz         |
| 2  | Ervin Howell     | Antonette | Shanna@melissa.tv         |
| 3  | Clementine Bauch | Samantha  | Nathan@yesenia.net        |
| 4  | Patricia Lebsack | Karianne  | Julianne.OConner@kory.org |
| 5  | Chelsey Dietrich | Kamren    | Lucio_Hettinger@annie.ca  |
+----+------------------+-----------+---------------------------+
*/

-- Retrieve posts for a specific user
SELECT id, title, body 
FROM jsonplaceholder.posts 
WHERE user_id_filter = 2 
LIMIT 2;

/*
+----+---------------------------------------+-----------------------------------------------------------------------------+
| id | title                                 | body                                                                        |
+----+---------------------------------------+-----------------------------------------------------------------------------+
| 11 | et ea vero quia laudantium autem      | delectus reiciendis molestiae occaecati non minima eveniet qui voluptatibus |
|    |                                       | accusamus in eum beatae sit                                                 |
|    |                                       | vel qui neque voluptates ut commodi qui incidunt                            |
|    |                                       | ut animi commodi                                                            |
| 12 | in quibusdam tempore odit est dolorem | itaque id aut magnam                                                        |
|    |                                       | praesentium quia et ea odit et ea voluptas et                               |
|    |                                       | sapiente quia nihil amet occaecati quia id voluptatem                       |
|    |                                       | incidunt ea est distinctio odio                                             |
+----+---------------------------------------+-----------------------------------------------------------------------------+
*/

-- Retrieve todos for a specific user
SELECT id, title, completed 
FROM jsonplaceholder.todos 
WHERE user_id_filter = 1 
LIMIT 3;

/*
+----+------------------------------------+-----------+
| id | title                              | completed |
+----+------------------------------------+-----------+
| 1  | delectus aut autem                 | false     |
| 2  | quis ut nam facilis et officia qui | false     |
| 3  | fugiat veniam minus                | false     |
+----+------------------------------------+-----------+
*/
```

## Local Testing

```bash
coral source add --file sources/community/jsonplaceholder/manifest.yaml
# Added source jsonplaceholder
# 
#   ✓ jsonplaceholder connected successfully
# 
#     jsonplaceholder (4 tables)
#     ├─ comments
#     ├─ posts
#     ├─ todos
#     └─ users
#     Query tests
#     3 declared · 3 passed · 0 failed
# 
#     ✓ SELECT id, name, email FROM jsonplaceholder.users LIMIT 1
#       1 row
# 
#     ✓ SELECT id, title, completed FROM jsonplaceholder.todos WHERE user_id_filter = 1 LIMIT 1
#       1 row
# 
#     ✓ SELECT id, name, email FROM jsonplaceholder.comments WHERE post_id_filter = 1 LIMIT 1
#       1 row

coral source test jsonplaceholder
#   ✓ jsonplaceholder connected successfully
# 
#     jsonplaceholder (4 tables)
#     ├─ comments
#     ├─ posts
#     ├─ todos
#     └─ users
#     Query tests
#     3 declared · 3 passed · 0 failed
# 
#     ✓ SELECT id, name, email FROM jsonplaceholder.users LIMIT 1
#       1 row
# 
#     ✓ SELECT id, title, completed FROM jsonplaceholder.todos WHERE user_id_filter = 1 LIMIT 1
#       1 row
# 
#     ✓ SELECT id, name, email FROM jsonplaceholder.comments WHERE post_id_filter = 1 LIMIT 1
#       1 row

coral sql "SELECT id, title, completed FROM jsonplaceholder.todos WHERE user_id_filter = 1 LIMIT 3"
# +----+------------------------------------+-----------+
# | id | title                              | completed |
# +----+------------------------------------+-----------+
# | 1  | delectus aut autem                 | false     |
# | 2  | quis ut nam facilis et officia qui | false     |
# | 3  | fugiat veniam minus                | false     |
# +----+------------------------------------+-----------+
```
