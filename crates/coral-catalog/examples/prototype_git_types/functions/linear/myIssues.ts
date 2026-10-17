/** Open Linear issues assigned to me, newest first. */
export default async function (input: { limit?: number }) {
  const result = await coral.providers.linear.listIssues({
    assignee: "me",
    orderBy: "updatedAt",
    limit: input.limit ?? 25,
  });
  return result.data.content.filter((block) => block.type === "text").map((block) => block.text);
}
