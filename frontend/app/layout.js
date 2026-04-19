import "./globals.css";

export const metadata = {
  title: "Big Data Query Tutor",
  description: "Learn SparkSQL, HiveQL, and Pig Latin with friendly syntax feedback."
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
