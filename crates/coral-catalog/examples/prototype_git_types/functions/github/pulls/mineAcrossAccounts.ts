export default async function (): Promise<Record<string, { total: number; items: Array<{ repo: string; number: number; title: string; draft: boolean; updated: string; url: string }> }>> {
  const pick = (items: any[]) => items.map((i: any) => ({
    repo: i.repository_url?.split("/repos/")[1],
    number: i.number,
    title: i.title,
    draft: i.draft ?? false,
    updated: i.updated_at,
    url: i.html_url,
  }));
  const queries = {
    authored: "is:pr is:open author:@me archived:false",
    review_requested: "is:pr is:open review-requested:@me archived:false",
  };
  const out: any = {};
  for (const [k, q] of Object.entries(queries)) {
    const a = await coral.providers.github.search.issuesAndPullRequests({ q, sort: "updated", order: "desc", per_page: 100 });
    const b = await coral.providers.github2.search.issuesAndPullRequests({ q, sort: "updated", order: "desc", per_page: 100 });
    out[`github_${k}`] = { total: (a.data as any).total_count, items: pick((a.data as any).items ?? []) };
    out[`github2_${k}`] = { total: (b.data as any).total_count, items: pick((b.data as any).items ?? []) };
  }
  return out;
}
