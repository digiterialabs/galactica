import type { NodeInfo } from "@/lib/types";

export function NodeCard({ node }: { node: NodeInfo }) {
  return (
    <div className="rounded-lg border bg-white p-4">
      <div className="flex items-center justify-between">
        <h3 className="font-medium">{node.hostname}</h3>
        <span className="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700">
          {node.status}
        </span>
      </div>
      <p className="mt-1 text-sm text-gray-500">{node.os} / {node.arch}</p>
    </div>
  );
}
