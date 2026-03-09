import type { ModelInfo } from "@/lib/types";

export function ModelCard({ model }: { model: ModelInfo }) {
  return (
    <div className="rounded-lg border bg-white p-4">
      <h3 className="font-medium">{model.name}</h3>
      <p className="mt-1 text-sm text-gray-500">{model.family}</p>
      <div className="mt-2 flex gap-1">
        {model.availableRuntimes.map((rt) => (
          <span key={rt} className="rounded bg-blue-100 px-2 py-0.5 text-xs text-blue-700">
            {rt}
          </span>
        ))}
      </div>
    </div>
  );
}
