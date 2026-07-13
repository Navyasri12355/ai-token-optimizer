import { FileText, Scissors, TrendingDown } from 'lucide-react';

export default function PromptComparison({ originalPrompt, optimizedPrompt, tokenSavings, compressionPercent, darkMode }) {
  return (
    <div className={`${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-100'} rounded-2xl shadow-lg border p-6 transition-colors duration-300`}>
      <h2 className={`text-xl font-bold ${darkMode ? 'text-gray-200' : 'text-gray-800'} mb-6 flex items-center gap-2`}>
        <Scissors className="w-6 h-6 text-purple-600" />
        Prompt Optimization
      </h2>
      
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Original Prompt */}
        <div className="space-y-3">
          <div className="flex items-center gap-2">
            <FileText className={`w-5 h-5 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`} />
            <h3 className={`font-semibold ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Original Prompt</h3>
          </div>
          <div className={`${darkMode ? 'bg-gray-700 border-gray-600' : 'bg-gray-50 border-gray-200'} rounded-xl p-4 border transition-colors duration-300`}>
            <p className={`${darkMode ? 'text-gray-200' : 'text-gray-700'} whitespace-pre-wrap`}>{originalPrompt}</p>
          </div>
        </div>

        {/* Optimized Prompt */}
        <div className="space-y-3">
          <div className="flex items-center gap-2">
            <Scissors className="w-5 h-5 text-green-600" />
            <h3 className={`font-semibold ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Optimized Prompt</h3>
          </div>
          <div className={`${darkMode ? 'bg-gray-700 border-gray-600' : 'bg-green-50 border-green-200'} rounded-xl p-4 border transition-colors duration-300`}>
            <p className={`${darkMode ? 'text-gray-200' : 'text-gray-700'} whitespace-pre-wrap`}>{optimizedPrompt}</p>
          </div>
        </div>
      </div>

      {/* Savings Metrics */}
      <div className="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className={`${darkMode ? 'bg-gray-700 border-gray-600' : 'bg-blue-50 border-blue-100'} rounded-xl p-4 border flex items-center gap-4 transition-colors duration-300`}>
          <div className={`${darkMode ? 'bg-gray-600' : 'bg-blue-100'} p-3 rounded-lg transition-colors duration-300`}>
            <TrendingDown className="w-6 h-6 text-blue-600" />
          </div>
          <div>
            <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>Token Savings</p>
            <p className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-blue-700'}`}>
              {tokenSavings || '0'}%
            </p>
          </div>
        </div>
        <div className={`${darkMode ? 'bg-gray-700 border-gray-600' : 'bg-purple-50 border-purple-100'} rounded-xl p-4 border flex items-center gap-4 transition-colors duration-300`}>
          <div className={`${darkMode ? 'bg-gray-600' : 'bg-purple-100'} p-3 rounded-lg transition-colors duration-300`}>
            <Scissors className="w-6 h-6 text-purple-600" />
          </div>
          <div>
            <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>Compression</p>
            <p className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-purple-700'}`}>
              {compressionPercent || '0'}%
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
