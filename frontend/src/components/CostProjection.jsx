import { useState } from 'react';
import { TrendingUp } from 'lucide-react';

export default function CostProjection({ baseCost, darkMode }) {
  const [turns, setTurns] = useState(3);

  const projectedCost = baseCost * turns;

  return (
    <div className={`${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-100'} rounded-2xl shadow-lg border p-6 transition-colors duration-300`}>
      <h2 className={`text-xl font-bold ${darkMode ? 'text-gray-200' : 'text-gray-800'} mb-6 flex items-center gap-2`}>
        <TrendingUp className="w-6 h-6 text-orange-600" />
        Multi-turn Cost Projection
      </h2>
      
      <div className="space-y-6">
        <div>
          <label className={`block text-sm font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'} mb-3`}>
            Number of conversation turns: {turns}
          </label>
          <input
            type="range"
            min="1"
            max="10"
            value={turns}
            onChange={(e) => setTurns(parseInt(e.target.value))}
            className={`w-full h-2 rounded-lg appearance-none cursor-pointer accent-blue-600 transition-colors duration-300 ${
              darkMode ? 'bg-gray-600' : 'bg-gray-200'
            }`}
          />
          <div className={`flex justify-between text-xs ${darkMode ? 'text-gray-500' : 'text-gray-500'} mt-1`}>
            <span>1</span>
            <span>10</span>
          </div>
        </div>

        <div className={`${darkMode ? 'bg-gray-700 border-gray-600' : 'bg-gradient-to-r from-orange-50 to-amber-50 border-orange-100'} rounded-xl p-6 border transition-colors duration-300`}>
          <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-600'} mb-2`}>
            Estimated cost for {turns} turns
          </p>
          <p className={`text-3xl font-bold ${darkMode ? 'text-white' : 'text-orange-700'}`}>
            ${projectedCost.toFixed(6)}
          </p>
        </div>
      </div>
    </div>
  );
}
