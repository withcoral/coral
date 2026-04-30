import { tableFromIPC } from "apache-arrow/Arrow.dom";
import { GrpcWebFetchTransport } from "@protobuf-ts/grpcweb-transport";
import { QueryServiceClient } from "./generated/coral/v1/query.client";
import { SourceServiceClient } from "./generated/coral/v1/sources.client";

const app = document.querySelector<HTMLDivElement>("#app");

if (!app) {
  throw new Error("Missing #app root");
}

app.textContent = "Coral reset UI";

const transport = new GrpcWebFetchTransport({
  baseUrl: window.location.origin,
  format: "binary",
});

const queryClient = new QueryServiceClient(transport);
const sourceClient = new SourceServiceClient(transport);

void queryClient
  .executeSql({
    workspace: { name: "default" },
    sql: "show tables;",
  })
  .response.then((response) => {
    const rows = arrowRowsFromSqlQuery(response.arrowIpcStream);
    console.log("show tables response", response);
    console.log("show tables rows", rows);
  })
  .catch((error: unknown) => {
    console.error("show tables failed", error);
  });

void queryClient
  .listTables({
    workspace: { name: "default" },
  })
  .response.then((response) => {
    console.log("listTables response", response);
    console.log("listTables tables", response.tables);
  })
  .catch((error: unknown) => {
    console.error("listTables failed", error);
  });

void sourceClient
  .listSources({
    workspace: { name: "default" },
  })
  .response.then((response) => {
    console.log("listSources response", response);
    console.log("listSources sources", response.sources);
  })
  .catch((error: unknown) => {
    console.error("listSources failed", error);
  });

function arrowRowsFromSqlQuery(arrowIpcStream: Uint8Array): Record<string, unknown>[] {
  const table = tableFromIPC(arrowIpcStream);
  return table.toArray().map((row) => row.toJSON() as Record<string, unknown>);
}
