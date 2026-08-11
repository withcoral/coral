pub(super) fn github_openapi() -> String {
    github_openapi_at_version("3.0.3")
}

/// The same fixture under a different specification version.
///
/// Nothing below is spelled differently across 3.0 and 3.1 — no `nullable`, no
/// `const`, no type arrays — so holding the document fixed and varying only the
/// version isolates the claim that the shared traversal reads both alike, and a
/// difference in the imported result is a difference the dialects introduced.
pub(super) fn github_openapi_at_version(version: &str) -> String {
    format!("openapi: {version}{GITHUB_OPENAPI_BODY}")
}

const GITHUB_OPENAPI_BODY: &str = r"
paths:
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: state, in: query, schema: {type: string}}
        - {name: page, in: query, schema: {type: integer}}
        - {name: per_page, in: query, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/issue'}
  /search/issues:
    get:
      operationId: search/issues-and-pull-requests
      parameters:
        - {name: q, in: query, required: true, schema: {type: string}}
        - {name: sort, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  items:
                    type: array
                    items: {$ref: '#/components/schemas/issue'}
  /repos/{owner}/{repo}/issues/{issue_number}:
    get:
      operationId: issues/get
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
        - {name: issue_number, in: path, required: true, schema: {type: integer}}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/issue'}
    patch:
      operationId: issues/update
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                title: {type: string}
      responses:
        '200':
          content:
            application/json:
              schema: {$ref: '#/components/schemas/issue'}
components:
  schemas:
    issue:
      type: object
      properties:
        id: {type: integer}
        number: {type: integer}
        title: {type: string}
        state: {type: string}
        html_url: {type: string}
        created_at: {type: string, format: date-time}
        updated_at: {type: string, format: date-time}
        body: {type: string}
        user: {type: object}
";
