import { ModelList } from "@/components/models/model-list";

export default function ModelsPage() {
  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Model Registry</h1>
      <ModelList />
    </div>
  );
}
