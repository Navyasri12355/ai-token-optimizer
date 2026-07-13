import { useState, useEffect } from 'react';
import { Zap, BarChart3, DollarSign, FileText, Scissors, TrendingDown, Moon, Sun } from 'lucide-react';
import PromptInput from './components/PromptInput';
import TokenBreakdown from './components/TokenBreakdown';
import CostAnalysis from './components/CostAnalysis';
import PromptComparison from './components/PromptComparison';
import TokenChart from './components/TokenChart';
import CostProjection from './components/CostProjection';
import { analyzePrompt } from './lib/api';
import { useTheme } from './context/ThemeContext';
import { MODELS, calculateCost } from './lib/models';

function App() {
  const [prompt, setPrompt] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [selectedModel, setSelectedModel] = useState(MODELS[0]);
  const { darkMode, toggleDarkMode } = useTheme();

  // Load sample data on mount to show populated dashboard
  useEffect(() => {
    const sampleData = {
      input_tokens: 1250,
      output_tokens: 890,
      total_tokens: 2140,
      estimated_cost: calculateCost(1250, 890, selectedModel),
      optimized_prompt: "Explain transformers in ML",
      token_savings_percent: 35.5,
      compression_percent: 28.2,
    };
    setResult(sampleData);
    setPrompt("Can you please explain to me in detail what transformers are in the context of machine learning and how they work?");
  }, [selectedModel]);

  const handleAnalyze = async () => {
    if (!prompt.trim()) {
      setError('Please enter a prompt.');
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const data = await analyzePrompt(prompt);
      // Recalculate cost based on selected model
      data.estimated_cost = calculateCost(data.input_tokens, data.output_tokens, selectedModel);
      setResult(data);
    } catch (err) {
      setError('API not running or error occurred. Please try again.');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className={`min-h-screen ${darkMode ? 'bg-gray-900' : 'bg-gradient-to-br from-blue-50 via-white to-purple-50'} transition-colors duration-300`}>
      {/* Header */}
      <header className={`${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'} border-b shadow-sm transition-colors duration-300`}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="bg-gradient-to-br from-blue-500 to-purple-600 p-3 rounded-xl shadow-lg">
                <Zap className="w-8 h-8 text-white" />
              </div>
              <div>
                <h1 className={`text-3xl font-bold bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent`}>
                  AI Token Optimizer
                </h1>
                <p className={`${darkMode ? 'text-gray-400' : 'text-gray-600'} mt-1 transition-colors duration-300`}>
                  Optimize prompts and predict LLM usage cost in real-time
                </p>
              </div>
            </div>
            
            {/* Theme Toggle */}
            <button
              onClick={toggleDarkMode}
              className={`p-3 rounded-xl transition-all duration-300 ${
                darkMode 
                  ? 'bg-gray-700 hover:bg-gray-600 text-yellow-400' 
                  : 'bg-gray-100 hover:bg-gray-200 text-gray-700'
              }`}
              aria-label="Toggle dark mode"
            >
              {darkMode ? <Sun className="w-6 h-6" /> : <Moon className="w-6 h-6" />}
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Input Section */}
        <PromptInput
          prompt={prompt}
          setPrompt={setPrompt}
          onAnalyze={handleAnalyze}
          loading={loading}
          darkMode={darkMode}
          selectedModel={selectedModel}
          setSelectedModel={setSelectedModel}
        />

        {/* Error Message */}
        {error && (
          <div className="mt-6 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-xl p-4 flex items-center gap-3">
            <div className="bg-red-100 dark:bg-red-800 p-2 rounded-lg">
              <Zap className="w-5 h-5 text-red-600 dark:text-red-400" />
            </div>
            <p className="text-red-700 dark:text-red-400 font-medium">{error}</p>
          </div>
        )}

        {/* Results Section */}
        {result && !loading && (
          <div className="mt-8 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            {/* Prompt Comparison */}
            <PromptComparison 
              originalPrompt={prompt} 
              optimizedPrompt={result.optimized_prompt} 
              tokenSavings={result.token_savings_percent}
              compressionPercent={result.compression_percent}
              darkMode={darkMode}
            />

            {/* Token Breakdown */}
            <TokenBreakdown data={result} darkMode={darkMode} />

            {/* Cost Analysis */}
            <CostAnalysis data={result} darkMode={darkMode} selectedModel={selectedModel} />

            {/* Token Chart */}
            <TokenChart data={result} darkMode={darkMode} />

            {/* Cost Projection */}
            <CostProjection baseCost={result.estimated_cost} darkMode={darkMode} />
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
