export function ClusterOverview() {
  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
      <StatCard title="Nodes" value="0" description="Active nodes" />
      <StatCard title="Models" value="0" description="Loaded models" />
      <StatCard title="Pools" value="0" description="Execution pools" />
    </div>
  );
}

function StatCard({
  title,
  value,
  description,
}: {
  title: string;
  value: string;
  description: string;
}) {
  return (
    <div className="rounded-lg border bg-white p-6">
      <p className="text-sm text-gray-500">{title}</p>
      <p className="mt-1 text-3xl font-bold">{value}</p>
      <p className="mt-1 text-sm text-gray-400">{description}</p>
    </div>
  );
}
