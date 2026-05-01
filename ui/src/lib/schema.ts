import { tableFromIPC } from "apache-arrow/Arrow.dom";
import { GrpcWebFetchTransport } from "@protobuf-ts/grpcweb-transport";
import { QueryServiceClient } from "@/generated/coral/v1/query.client";
import type { Table } from "@/generated/coral/v1/catalog";

export interface Column {
  name: string;
  type: string;
  filterable: boolean;
  virtual: boolean;
  description?: string;
}

export interface TableDef {
  name: string;
  description?: string;
  requiredFilters?: string;
  guide?: string;
  columns: Column[];
}

export interface PluginSchema {
  name: string;
  tables: TableDef[];
}

export interface SchemaResponse {
  connectors: PluginSchema[];
}

const queryClient = new QueryServiceClient(
  new GrpcWebFetchTransport({
    baseUrl: window.location.origin,
    format: "binary",
  }),
);

export async function fetchSchemaFromCoral(): Promise<SchemaResponse> {
  const { response } = await queryClient.listTables({
    workspace: { name: "default" },
  });

  const connectorMap = new Map<string, TableDef[]>();
  for (const row of response.tables) {
    if (!connectorMap.has(row.schemaName)) connectorMap.set(row.schemaName, []);
    connectorMap.get(row.schemaName)?.push(mapTable(row));
  }

  return {
    connectors: Array.from(connectorMap.entries()).map(([name, tables]) => ({ name, tables })),
  };
}

export async function executeSchemaQuery(sql: string): Promise<{ rows?: Record<string, unknown>[]; columns?: string[]; error?: string }> {
  try {
    const { response } = await queryClient.executeSql({
      workspace: { name: "default" },
      sql,
    });
    const table = tableFromIPC(response.arrowIpcStream);
    return {
      columns: table.schema.fields.map((field) => field.name),
      rows: table.toArray().map((row) => row.toJSON() as Record<string, unknown>),
    };
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) };
  }
}

function mapTable(row: Table): TableDef {
  return {
    name: row.name,
    description: row.description || undefined,
    requiredFilters: row.requiredFilters.join(", ") || undefined,
    columns: row.columns.map((column) => ({
      name: column.name,
      type: column.dataType,
      filterable: row.requiredFilters.includes(column.name),
      virtual: false,
      description: undefined,
    })),
  };
}
