import type { TableDef } from "../lib/schema";

export function requiredFilterSet(table: TableDef): Set<string> {
  if (!table.requiredFilters) return new Set();
  return new Set(table.requiredFilters.split(",").map((s) => s.trim()).filter(Boolean));
}

export function buildDefaultQuery(connector: string, table: TableDef): string {
  const base = `SELECT * FROM ${connector}.${table.name}`;
  const requiredNames = [...requiredFilterSet(table)];
  if (requiredNames.length > 0) {
    const where = requiredNames.map((c) => `"${c}" = '<${c}>'`).join(" AND ");
    return `${base} WHERE ${where} LIMIT 10`;
  }
  return `${base} LIMIT 10`;
}
