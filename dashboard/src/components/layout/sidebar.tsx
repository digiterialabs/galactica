import Link from "next/link";

const navigation = [
  { name: "Overview", href: "/" },
  { name: "Nodes", href: "/nodes" },
  { name: "Models", href: "/models" },
  { name: "Inference", href: "/inference" },
  { name: "Settings", href: "/settings" },
];

export function Sidebar() {
  return (
    <aside className="flex w-56 flex-col border-r bg-white">
      <div className="flex h-14 items-center border-b px-4">
        <span className="text-lg font-bold">Galactica</span>
      </div>
      <nav className="flex-1 p-2">
        {navigation.map((item) => (
          <Link
            key={item.name}
            href={item.href}
            className="block rounded-md px-3 py-2 text-sm text-gray-700 hover:bg-gray-100"
          >
            {item.name}
          </Link>
        ))}
      </nav>
    </aside>
  );
}
