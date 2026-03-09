import { NodeList } from "@/components/nodes/node-list";

export default function NodesPage() {
  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Nodes</h1>
      <NodeList />
    </div>
  );
}
