export function Header() {
  return (
    <header className="flex h-14 items-center border-b bg-white px-6">
      <h2 className="text-lg font-semibold">Galactica</h2>
      <div className="ml-auto flex items-center gap-4">
        <span className="text-sm text-gray-500">v0.1.0</span>
      </div>
    </header>
  );
}
