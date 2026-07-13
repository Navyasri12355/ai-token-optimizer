import { BarChart3 } from 'lucide-react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

export default function TokenChart({ data, darkMode }) {
  const chartData = [
    {
      name: 'Input Tokens',
      value: data.input_tokens,
      fill: '#3b82f6',
    },
    {
      name: 'Output Tokens',
      value: data.output_tokens,
      fill: '#a855f7',
    },
  ];

  return (
    <div className={`${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-100'} rounded-2xl shadow-lg border p-6 transition-colors duration-300`}>
      <h2 className={`text-xl font-bold ${darkMode ? 'text-gray-200' : 'text-gray-800'} mb-6 flex items-center gap-2`}>
        <BarChart3 className="w-6 h-6 text-indigo-600" />
        Token Distribution
      </h2>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke={darkMode ? '#374151' : '#e5e7eb'} />
            <XAxis 
              dataKey="name" 
              axisLine={false}
              tickLine={false}
              tick={{ fill: darkMode ? '#9ca3af' : '#6b7280', fontSize: 14 }}
            />
            <YAxis 
              axisLine={false}
              tickLine={false}
              tick={{ fill: darkMode ? '#9ca3af' : '#6b7280', fontSize: 14 }}
            />
            <Tooltip 
              contentStyle={{
                backgroundColor: 'white',
                border: '1px solid #e5e7eb',
                borderRadius: '8px',
                boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
              }}
              formatter={(value) => [value.toLocaleString(), 'Tokens']}
            />
            <Bar dataKey="value" radius={[8, 8, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
