import { useEffect, useState } from 'react';
export default function Home() {
  const [data, setData] = useState([]);
  useEffect(() => {
    fetch('/api/data')
      .then(res => res.json())
      .then(setData);
  }, []);
  return (
    <div className="p-6 bg-gray-50 min-h-screen">
      <h1 className="text-2xl font-bold mb-4">Lakehouse Realtime Pipeline</h1>
      <div className="grid grid-cols-2 gap-4">
        {data.map((item, i) => (
          <div key={i} className="bg-white p-4 rounded shadow">
            <p><strong>Order ID:</strong> {item.order_id}</p>
            <p><strong>Amount:</strong> ${item.amount}</p>
            <p><strong>Segment:</strong> {item.value_segment}</p>
          </div>
        ))}
      </div>
    </div>
  );
}