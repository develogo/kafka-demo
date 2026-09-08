import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Order Saga",
  description: "Place an order and watch it move through Kafka",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
