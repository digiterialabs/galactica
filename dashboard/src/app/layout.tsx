import type { Metadata } from "next";

import "./globals.css";

export const metadata: Metadata = {
  title: "Galactica Dashboard",
  description: "Real-time cluster operations dashboard for the Galactica platform.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
