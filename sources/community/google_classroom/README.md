# Google Classroom community source

The `google_classroom` community source exposes read-only Google Classroom course, coursework (assignments), and student/teacher roster data through Coral SQL.

## Setup

Google Classroom API requires OAuth 2.0 user authentication with appropriate scopes.

To use the Google Classroom source:
1. Ensure Classroom API is enabled in Google Cloud Console.
2. Obtain an OAuth 2.0 access token requesting the following read-only scopes:
   * `https://www.googleapis.com/auth/classroom.courses.readonly`
   * `https://www.googleapis.com/auth/classroom.coursework.students.readonly`
   * `https://www.googleapis.com/auth/classroom.coursework.me.readonly`
   * `https://www.googleapis.com/auth/classroom.rosters.readonly`
   * `https://www.googleapis.com/auth/classroom.profile.emails`
   * `https://www.googleapis.com/auth/classroom.profile.photos`
3. Install the source:

```sh
export GOOGLE_CLASSROOM_ACCESS_TOKEN="<oauth-access-token>"
cargo run -p coral-cli -- source add --file sources/community/google_classroom/manifest.yaml
```

Alternatively, you can run the interactive setup flow in the Coral UI by selecting **Connect Google Classroom** and providing your Google Cloud OAuth Client ID and Secret.

## Tables

| Table | Purpose | Required Filters |
| --- | --- | --- |
| `google_classroom.courses` | Lists courses that the user is enrolled in or teaching. | None |
| `google_classroom.coursework` | Assignments or questions assigned to students in a course. | `course_id` |
| `google_classroom.students` | Students enrolled in a course. | `course_id` |
| `google_classroom.teachers` | Instructors teaching a course. | `course_id` |

All tables are read-only. This source does not create, update, delete, or grade coursework.

### Important Design Quirks

* **Required Filters**: The `coursework`, `students`, and `teachers` tables represent nested sub-resources and require a `course_id` filter in the SQL `WHERE` clause.
* **Fragmented Due Date**: Upstream deadlines are not returned as standard ISO 8601 strings. Instead, they are split into separate year/month/day and hour/minute fields. They are exposed as raw JSON columns `due_date` and `due_time` and require manual reconstruction for temporal SQL predicates.
* **Profile Email & Photo Availability**: The `email_address` and `photo_url` columns on the `students` and `teachers` tables will return `null` unless the specific email/photo profile scopes have been granted.

## Example queries

Discover courses:

```sql
SELECT id, name, section, course_state
FROM google_classroom.courses
LIMIT 20;
```

List coursework/assignments within a course:

```sql
SELECT id, title, state, work_type, max_points, due_date
FROM google_classroom.coursework
WHERE course_id = 'course_12345'
LIMIT 50;
```

List student rosters for a specific course:

```sql
SELECT user_id, full_name, email_address
FROM google_classroom.students
WHERE course_id = 'course_12345'
ORDER BY full_name;
```

List instructors for a specific course:

```sql
SELECT user_id, full_name, email_address
FROM google_classroom.teachers
WHERE course_id = 'course_12345'
ORDER BY full_name;
```

## Validation

Lint the manifest:

```sh
cargo run -p coral-cli -- source lint sources/community/google_classroom/manifest.yaml
```

Install and test with a real or mock token:

```sh
export GOOGLE_CLASSROOM_ACCESS_TOKEN="<token>"
cargo run -p coral-cli -- source add --file sources/community/google_classroom/manifest.yaml
cargo run -p coral-cli -- source test google_classroom
```

Inspect the registered source metadata:

```sh
cargo run -p coral-cli -- sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'google_classroom'"
cargo run -p coral-cli -- sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'google_classroom' ORDER BY table_name, ordinal_position"
```
