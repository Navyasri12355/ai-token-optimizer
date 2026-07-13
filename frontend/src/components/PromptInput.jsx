import { Send, Loader2, Cpu } from 'lucide-react';
import { MODELS } from '../lib/models';

export default function PromptInput({ prompt, setPrompt, onAnalyze, loading, darkMode, selectedModel, setSelectedModel }) {
  return (
    <div className={`${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-100'} rounded-2xl shadow-lg border p-6 transition-colors duration-300`}>
      {/* Model Selector */}
      <div className="mb-4">
        <label className={`block text-sm font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'} mb-2 flex items-center gap-2`}>
          <Cpu className="w-4 h-4" />
          Select Model
        </label>
        <select
          value={selectedModel.id}
          onChange={(e) => setSelectedModel(MODELS.find(m => m.id === e.target.value))}
          disabled={loading}
          className={`w-full px-4 py-2.5 rounded-xl border focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200 ${
            darkMode 
              ? 'bg-gray-700 border-gray-600 text-gray-200 focus:ring-blue-400' 
              : 'bg-white border-gray-300 text-gray-700'
          } disabled:opacity-50 disabled:cursor-not-allowed`}
        >
          {MODELS.map((model) => (
            <option key={model.id} value={model.id} className="bg-white text-gray-900">
              {model.name} - ${model.inputPricePer1K}/1K input, ${model.outputPricePer1K}/1K output
            </option>
          ))}
        </select>
        <p className={`text-xs mt-1 ${darkMode ? 'text-gray-500' : 'text-gray-500'}`}>
          {selectedModel.description}
        </p>
      </div>

      <label className={`block text-lg font-semibold ${darkMode ? 'text-gray-200' : 'text-gray-800'} mb-3 flex items-center gap-2`}>
        <span className="text-2xl">📝</span>
        Enter your prompt
      </label>
      <textarea
        value={prompt}
        onChange={(e) => setPrompt(e.target.value)}
        placeholder="Type or paste your prompt here..."
        className={`w-full h-40 px-4 py-3 border rounded-xl focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-none transition-all duration-200 ${
          darkMode 
            ? 'bg-gray-700 border-gray-600 text-gray-200 placeholder-gray-500 focus:ring-blue-400' 
            : 'bg-white border-gray-300 text-gray-700 placeholder-gray-400'
        }`}
        disabled={loading}
      />
      <button
        onClick={onAnalyze}
        disabled={loading || !prompt.trim()}
        className="mt-4 w-full bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-700 hover:to-purple-700 text-white font-semibold py-3 px-6 rounded-xl transition-all duration-200 flex items-center justify-center gap-2 shadow-md hover:shadow-lg disabled:opacity-50 disabled:cursor-not-allowed disabled:shadow-none"
      >
        {loading ? (
          <>
            <Loader2 className="w-5 h-5 animate-spin" />
            Analyzing...
          </>
        ) : (
          <>
            <Send className="w-5 h-5" />
            Analyze
          </>
        )}
      </button>
    </div>
  );
}
