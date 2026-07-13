import { DollarSign } from 'lucide-react';

export default function CostAnalysis({ data, darkMode }) {
  return (
    <div className={`${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-100'} rounded-2xl shadow-lg border p-6 transition-colors duration-300`}>
      <h2 className={`text-xl font-bold ${darkMode ? 'text-gray-200' : 'text-gray-800'} mb-6 flex items-center gap-2`}>
        <DollarSign className="w-6 h-6 text-green-600" />
        Cost Analysis
      </h2>
      <div className={`${darkMode ? 'bg-gray-700 border-gray-600' : 'bg-gradient-to-r from-green-50 to-emerald-50 border-green-100'} rounded-xl p-8 border transition-colors duration-300`}>
        <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-600'} mb-2`}>Estimated Cost</p>
        <p className={`text-4xl font-bold ${darkMode ? 'text-white' : 'text-green-700'}`}>
          ${data.estimated_cost.toFixed(6)}
        </p>
        <p className={`text-sm ${darkMode ? 'text-gray-500' : 'text-gray-500'} mt-2`}>Based on GPT-3.5-turbo pricing</p>
      </div>
    </div>
  );
}
