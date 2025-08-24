export default function DashboardCard({ title, value }) {
  return (
    <div className="bg-white p-4 rounded shadow text-center">
      <h3 className="font-semibold text-gray-700">{title}</h3>
      <p className="text-2xl font-bold text-blue-600">{value}</p>
    </div>
  );
}