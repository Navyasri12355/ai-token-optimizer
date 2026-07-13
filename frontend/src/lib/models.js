export const MODELS = [
  {
    id: 'gpt-5-6-sol',
    name: 'gpt-5.6-sol',
    provider: 'OpenAI',
    inputPricePer1K: 0.005,
    outputPricePer1K: 0.03,
    description: 'GPT-5.6 Sol model'
  },
  {
    id: 'gpt-5-6-terra',
    name: 'gpt-5.6-terra',
    provider: 'OpenAI',
    inputPricePer1K: 0.0025,
    outputPricePer1K: 0.015,
    description: 'GPT-5.6 Terra model'
  },
  {
    id: 'gpt-5-6-luna',
    name: 'gpt-5.6-luna',
    provider: 'OpenAI',
    inputPricePer1K: 0.001,
    outputPricePer1K: 0.006,
    description: 'GPT-5.6 Luna model'
  },
  {
    id: 'gpt-5-5',
    name: 'gpt-5.5',
    provider: 'OpenAI',
    inputPricePer1K: 0.005,
    outputPricePer1K: 0.03,
    description: 'GPT-5.5 model'
  },
  {
    id: 'gpt-5-5-pro',
    name: 'gpt-5.5-pro',
    provider: 'OpenAI',
    inputPricePer1K: 0.03,
    outputPricePer1K: 0.18,
    description: 'GPT-5.5 Pro model'
  },
  {
    id: 'gpt-5-4',
    name: 'gpt-5.4',
    provider: 'OpenAI',
    inputPricePer1K: 0.0025,
    outputPricePer1K: 0.015,
    description: 'GPT-5.4 model'
  },
  {
    id: 'gpt-5-4-mini',
    name: 'gpt-5.4-mini',
    provider: 'OpenAI',
    inputPricePer1K: 0.00075,
    outputPricePer1K: 0.0045,
    description: 'GPT-5.4 Mini model'
  },
  {
    id: 'gpt-5-4-nano',
    name: 'gpt-5.4-nano',
    provider: 'OpenAI',
    inputPricePer1K: 0.0002,
    outputPricePer1K: 0.00125,
    description: 'GPT-5.4 Nano model'
  },
  {
    id: 'gpt-5-4-pro',
    name: 'gpt-5.4-pro',
    provider: 'OpenAI',
    inputPricePer1K: 0.03,
    outputPricePer1K: 0.18,
    description: 'GPT-5.4 Pro model'
  },
  {
    id: 'claude-opus-4-8',
    name: 'Claude Opus 4.8',
    provider: 'Anthropic',
    inputPricePer1K: 0.005,
    outputPricePer1K: 0.025,
    description: 'High-end model for complex tasks'
  },
  {
    id: 'claude-sonnet-5',
    name: 'Claude Sonnet 5',
    provider: 'Anthropic',
    inputPricePer1K: 0.002,
    outputPricePer1K: 0.01,
    description: 'Latest model with balanced performance (introductory pricing until Aug 31, 2026)'
  },
  {
    id: 'claude-sonnet-4-6',
    name: 'Claude Sonnet 4.6',
    provider: 'Anthropic',
    inputPricePer1K: 0.003,
    outputPricePer1K: 0.015,
    description: 'Balanced performance and speed'
  },
  {
    id: 'claude-haiku-4-5',
    name: 'Claude Haiku 4.5',
    provider: 'Anthropic',
    inputPricePer1K: 0.001,
    outputPricePer1K: 0.005,
    description: 'Fast and lightweight for simple tasks'
  },
  {
    id: 'gemini-3-1-pro-preview',
    name: 'Gemini 3.1 Pro Preview',
    provider: 'Google',
    inputPricePer1K: 0.002,
    outputPricePer1K: 0.012,
    description: 'Current Pro flagship'
  },
  {
    id: 'gemini-3-5-flash',
    name: 'Gemini 3.5 Flash',
    provider: 'Google',
    inputPricePer1K: 0.0015,
    outputPricePer1K: 0.009,
    description: 'Current Flash flagship (Free tier)'
  }
];

export function getModelById(id) {
  return MODELS.find(model => model.id === id) || MODELS[0];
}

export function calculateCost(inputTokens, outputTokens, model) {
  const inputCost = (inputTokens / 1000) * model.inputPricePer1K;
  const outputCost = (outputTokens / 1000) * model.outputPricePer1K;
  return inputCost + outputCost;
}
