# Google Classroom community source

The `google_classroom` community source exposes read-only Google Classroom course, coursework (assignments), and student/teacher roster data through Coral SQL.

## Setup

Google Classroom API requires OAuth 2.0 user authentication with the following read-only scopes:
* `https://www.googleapis.com/auth/classroom.courses.readonly` (read course metadata)
* `https://www.googleapis.com/auth/classroom.coursework.students.readonly` (read course coursework)
* `https://www.googleapis.com/auth/classroom.coursework.me.readonly` (read student/teacher submissions)
* `https://www.googleapis.com/auth/classroom.rosters.readonly` (read course roster)
* `https://www.googleapis.com/auth/classroom.profile.emails` (optional, read user email address profiles)
* `https://www.googleapis.com/auth/classroom.profile.photos` (optional, read user profile photo URLs)

### 🔑 OAuth Credentials Configuration

To connect Google Classroom, you must configure a Google Cloud Console project:
1. Open the [Google Cloud Console](https://console.cloud.google.com/).
2. Enable the **Google Classroom API** under **APIs & Services**.
3. Configure the **OAuth Consent Screen**:
   * Set User Type to **External** (unless you are in a Google Workspace organization).
   * Under **Publishing status**, keep it in **Testing**.
   * Under **Test Users**, add your own Google email address (this is required to log in while the app is in testing).
4. Go to **Credentials**, click **Create Credentials** -> **OAuth client ID**.
5. **CRITICAL**: Set the application type to **Desktop app**. (Do **NOT** select Web application. Coral runs a local loopback server at `http://127.0.0.1:<random_port>` to handle the redirect. Google only allows dynamic loopback port redirection for Desktop app credentials).

### 🚀 Connection Options

#### Option A: Guided OAuth Connection (Recommended)
This uses your Desktop app Client ID/Secret. The OAuth flow requests offline access (`access_type=offline`) so Google returns refresh-token metadata. Note that because automatic background token refresh is not yet fully implemented in Coral, you may still need to reconnect or re-add the source when the stored access token expires (typically after 1 hour).
Run:
```sh
coral source add --file sources/community/google_classroom/manifest.yaml --interactive
```
Select **Connect Google Classroom** and provide your Client ID and Client Secret.

#### Option B: Pasted Access Token
You can paste a pre-generated access token.
> [!WARNING]
> Like the guided OAuth flow, pasted access tokens are short-lived and will expire in **1 hour**. Once expired, you will need to re-generate the token and re-add the source.

To connect:
```sh
export GOOGLE_CLASSROOM_ACCESS_TOKEN="<oauth-access-token>"
coral source add --file sources/community/google_classroom/manifest.yaml
```

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
* **Mutually Exclusive Filters**: On the `courses` table, the `student_id` and `teacher_id` filters are mutually exclusive. Querying both together will trigger a bad request error from the Classroom API.
* **Coursework Default State**: The Classroom API coursework endpoint defaults to returning only `PUBLISHED` coursework. To query drafts or deleted coursework, filter explicitly using `course_work_state` (e.g. `WHERE course_work_state = 'DRAFT'`).
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
coral source lint sources/community/google_classroom/manifest.yaml
```

Install and test with a real or mock token:

```sh
export GOOGLE_CLASSROOM_ACCESS_TOKEN="<token>"
coral source add --file sources/community/google_classroom/manifest.yaml
coral source test google_classroom
```

Inspect the registered source metadata:

```sh
coral sql "SELECT table_name, description FROM coral.tables WHERE schema_name = 'google_classroom'"
coral sql "SELECT table_name, column_name, data_type FROM coral.columns WHERE schema_name = 'google_classroom' ORDER BY table_name, ordinal_position"
```
