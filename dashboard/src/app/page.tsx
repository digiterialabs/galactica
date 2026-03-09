import { ClusterOverview } from "@/components/cluster/cluster-overview";

export default function HomePage() {
  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Cluster Overview</h1>
      <ClusterOverview />
    </div>
  );
}
