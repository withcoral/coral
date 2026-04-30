// Keep the browser bundle on Arrow's DOM entrypoint; the package also ships
// Node stream/CLI entrypoints that should not be resolved into the UI.
import { tableFromIPC, type Table } from "apache-arrow/Arrow.dom";
import { GrpcWebFetchTransport } from "@protobuf-ts/grpcweb-transport";
import { QueryServiceClient } from "./generated/coral/v1/query.client";
import type { ExecuteSqlResponse } from "./generated/coral/v1/query";
import "./styles.css";

const DEFAULT_SQL = "select * from coral.tables limit 20";

type QueryState = "idle" | "running" | "success" | "error";

type QueryResult = {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
};

const queryClient = new QueryServiceClient(
  new GrpcWebFetchTransport({
    baseUrl: window.location.origin,
    format: "binary",
  }),
);

const app = document.querySelector<HTMLDivElement>("#app");

if (!app) {
  throw new Error("Missing #app root");
}

app.innerHTML = `
  <main class="shell">
    <section class="query-panel" aria-labelledby="query-title">
      <div class="panel-header">
        <div>
          <p class="eyebrow">Coral</p>
          <h1 id="query-title">SQL Console</h1>
        </div>
        <div class="status" data-state="idle" id="query-status">Ready</div>
      </div>

      <form id="query-form" class="query-form">
        <label for="sql-input">SQL</label>
        <textarea id="sql-input" spellcheck="false">${DEFAULT_SQL}</textarea>
        <div class="actions">
          <button id="execute-button" type="submit">Execute</button>
        </div>
      </form>
    </section>

    <section class="results-panel" aria-labelledby="result-title">
      <div class="result-header">
        <div>
          <p class="eyebrow">Result</p>
          <h2 id="result-title">Rows</h2>
        </div>
        <div class="row-count" id="row-count">0 rows</div>
      </div>
      <div id="error-message" class="error-message" hidden></div>
      <div id="empty-state" class="empty-state">No query has been executed.</div>
      <div id="table-wrap" class="table-wrap" hidden>
        <table>
          <thead id="result-head"></thead>
          <tbody id="result-body"></tbody>
        </table>
      </div>
    </section>
  </main>
`;

const form = requiredElement<HTMLFormElement>("#query-form");
const sqlInput = requiredElement<HTMLTextAreaElement>("#sql-input");
const executeButton = requiredElement<HTMLButtonElement>("#execute-button");
const statusElement = requiredElement<HTMLDivElement>("#query-status");
const rowCountElement = requiredElement<HTMLDivElement>("#row-count");
const errorElement = requiredElement<HTMLDivElement>("#error-message");
const emptyElement = requiredElement<HTMLDivElement>("#empty-state");
const tableWrap = requiredElement<HTMLDivElement>("#table-wrap");
const tableHead =
  requiredElement<HTMLTableSectionElement>("#result-head");
const tableBody =
  requiredElement<HTMLTableSectionElement>("#result-body");

form.addEventListener("submit", (event) => {
  event.preventDefault();
  void executeSql(sqlInput.value);
});

function requiredElement<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) {
    throw new Error(`Missing required element: ${selector}`);
  }
  return element;
}

async function executeSql(sql: string): Promise<void> {
  const statement = sql.trim();
  if (!statement) {
    renderError("Enter a SQL statement.");
    return;
  }

  setQueryState("running", "Running");
  executeButton.disabled = true;

  try {
    const response = await executeSqlRequest(statement);
    const result = decodeQueryResult(response);
    renderResult(result);
    setQueryState("success", "Complete");
  } catch (error) {
    renderError(error instanceof Error ? error.message : String(error));
    setQueryState("error", "Error");
  } finally {
    executeButton.disabled = false;
  }
}

async function executeSqlRequest(sql: string): Promise<ExecuteSqlResponse> {
  const { response } = await queryClient.executeSql({
    workspace: { name: "default" },
    sql,
  });
  return response;
}

function decodeQueryResult(response: ExecuteSqlResponse): QueryResult {
  const table = tableFromIPC(response.arrowIpcStream);
  const columns = table.schema.fields.map((field) => field.name);
  const rows = tableRows(table, columns);
  return {
    columns,
    rows,
    rowCount: response.rowCount,
  };
}

function tableRows(
  table: Table,
  columns: string[],
): Record<string, unknown>[] {
  return table.toArray().map((row) => {
    const json = row.toJSON() as Record<string, unknown>;
    return Object.fromEntries(columns.map((column) => [column, json[column]]));
  });
}

function renderResult(result: QueryResult): void {
  errorElement.hidden = true;
  errorElement.textContent = "";
  rowCountElement.textContent = formatRowCount(result.rowCount);

  tableHead.replaceChildren();
  tableBody.replaceChildren();

  if (result.columns.length === 0) {
    tableWrap.hidden = true;
    emptyElement.hidden = false;
    emptyElement.textContent = "The query returned no columns.";
    return;
  }

  const headerRow = document.createElement("tr");
  for (const column of result.columns) {
    const th = document.createElement("th");
    th.scope = "col";
    th.textContent = column;
    headerRow.append(th);
  }
  tableHead.append(headerRow);

  for (const row of result.rows) {
    const tableRow = document.createElement("tr");
    for (const column of result.columns) {
      const cell = document.createElement("td");
      const value = row[column];
      cell.textContent = formatCellValue(value);
      if (value === null || value === undefined) {
        cell.classList.add("null-cell");
      }
      tableRow.append(cell);
    }
    tableBody.append(tableRow);
  }

  if (result.rows.length === 0) {
    const tableRow = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = result.columns.length;
    cell.textContent = "No rows returned.";
    cell.className = "empty-table-cell";
    tableRow.append(cell);
    tableBody.append(tableRow);
  }

  emptyElement.hidden = true;
  tableWrap.hidden = false;
}

function renderError(message: string): void {
  tableWrap.hidden = true;
  emptyElement.hidden = true;
  rowCountElement.textContent = "0 rows";
  errorElement.hidden = false;
  errorElement.textContent = message;
}

function setQueryState(state: QueryState, label: string): void {
  statusElement.dataset.state = state;
  statusElement.textContent = label;
}

function formatRowCount(count: number): string {
  return `${count.toLocaleString()} ${count === 1 ? "row" : "rows"}`;
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "NULL";
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}
