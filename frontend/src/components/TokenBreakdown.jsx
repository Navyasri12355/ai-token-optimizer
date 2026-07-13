import { BarChart3 } from 'lucide-react';

export default function TokenBreakdown({ data, darkMode }) {
  const metrics = [
    {
      label: 'Input Tokens',
      value: data.input_tokens,
      color: 'from-blue-500 to-blue-600',
      bgColor: 'bg-blue-50',
      textColor: 'text-blue-700',
    },
    {
      label: 'Output Tokens',
      value: data.output_tokens,
      color: 'from-purple-500 to-purple-600',
      bgColor: 'bg-purple-50',
      textColor: 'text-purple-700',
    },
    {
      label: 'Total Tokens',
      value: data.total_tokens,
      color: 'from-indigo-500 to-indigo-600',
      bgColor: 'bg-indigo-50',
      textColor: 'text-indigo-700',
    },
  ];

  return (
    <div className={`${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-100'} rounded-2xl shadow-lg border p-6 transition-colors duration-300`}>
      <h2 className={`text-xl font-bold ${darkMode ? 'text-gray-200' : 'text-gray-800'} mb-6 flex items-center gap-2`}>
        <BarChart3 className="w-6 h-6 text-blue-600" />
        Token Breakdown
      </h2>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {metrics.map((metric) => (
          <div
            key={metric.label}
            className={`${darkMode ? 'bg-gray-700 border-gray-600' : metric.bgColor + ' border-gray-100'} rounded-xl p-6 border transition-all duration-200 hover:shadow-md`}
          >
            <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-600'} mb-2`}>{metric.label}</p>
            <p className={`text-3xl font-bold ${darkMode ? 'text-white' : metric.textColor}`}>
              {metric.value.toLocaleString()}
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}
