export const MODELS = [
  // NEW MODELS (Latest)
  {
    id: 'gpt-6-astra',
    name: 'GPT-6 Astra',
    provider: 'OpenAI',
    inputPricePer1K: 0.010,
    outputPricePer1K: 0.050,
    description: 'Advanced reasoning and complex problem-solving'
  },
  {
    id: 'gpt-6-1-sol',
    name: 'GPT-6.1 Sol',
    provider: 'OpenAI',
    inputPricePer1K: 0.002,
    outputPricePer1K: 0.010,
    description: 'Balanced reasoning and general-purpose performance'
  },
  {
    id: 'gpt-6-1-terra',
    name: 'GPT-6.1 Terra',
    provider: 'OpenAI',
    inputPricePer1K: 0.001,
    outputPricePer1K: 0.005,
    description: 'Efficient reasoning at a lower cost'
  },
  {
    id: 'gpt-6-luna',
    name: 'GPT-6 Luna',
    provider: 'OpenAI',
    inputPricePer1K: 0.0001,
    outputPricePer1K: 0.0005,
    description: 'Fast and lightweight for simple tasks'
  },

  // ANTHROPIC MODELS (Latest)
  {
    id: 'claude-opus-5-5',
    name: 'Claude Opus 5.5',
    provider: 'Anthropic',
    inputPricePer1K: 0.004,
    outputPricePer1K: 0.020,
    description: 'High-end model for complex tasks'
  },
  {
    id: 'claude-sonnet-5-5',
    name: 'Claude Sonnet 5.5',
    provider: 'Anthropic',
    inputPricePer1K: 0.002,
    outputPricePer1K: 0.010,
    description: 'Balanced performance and speed'
  },
  {
    id: 'claude-haiku-5-5',
    name: 'Claude Haiku 5.5',
    provider: 'Anthropic',
    inputPricePer1K: 0.0001,
    outputPricePer1K: 0.0005,
    description: 'Fast and lightweight for simple tasks'
  },

  // GOOGLE MODELS (Latest)
  {
    id: 'gemini-4-argon',
    name: 'Gemini 4 Argon',
    provider: 'Google',
    inputPricePer1K: 0.002,
    outputPricePer1K: 0.010,
    description: 'Next-generation Gemini model'
  },

  // LEGACY MODELS

  // OPENAI
  {
    id: 'gpt-5-6-sol',
    name: 'GPT-5.6 Sol',
    provider: 'OpenAI',
    inputPricePer1K: 0.005,
    outputPricePer1K: 0.03,
    description: 'Advanced reasoning and coding capabilities'
  },
  {
    id: 'gpt-5-6-terra',
    name: 'GPT-5.6 Terra',
    provider: 'OpenAI',
    inputPricePer1K: 0.0025,
    outputPricePer1K: 0.015,
    description: 'Balanced performance and efficiency'
  },
  {
    id: 'gpt-5-6-luna',
    name: 'GPT-5.6 Luna',
    provider: 'OpenAI',
    inputPricePer1K: 0.001,
    outputPricePer1K: 0.006,
    description: 'Lightweight model for quick responses'
  },
  {
    id: 'gpt-5-5',
    name: 'GPT-5.5',
    provider: 'OpenAI',
    inputPricePer1K: 0.005,
    outputPricePer1K: 0.03,
    description: 'General-purpose reasoning and assistance'
  },
  {
    id: 'gpt-5-5-pro',
    name: 'GPT-5.5 Pro',
    provider: 'OpenAI',
    inputPricePer1K: 0.03,
    outputPricePer1K: 0.18,
    description: 'Premium model for complex reasoning tasks'
  },
  {
    id: 'gpt-5-4',
    name: 'GPT-5.4',
    provider: 'OpenAI',
    inputPricePer1K: 0.0025,
    outputPricePer1K: 0.015,
    description: 'Versatile model for coding and analysis'
  },
  {
    id: 'gpt-5-4-mini',
    name: 'GPT-5.4 Mini',
    provider: 'OpenAI',
    inputPricePer1K: 0.00075,
    outputPricePer1K: 0.0045,
    description: 'Efficient performance for everyday tasks'
  },
  {
    id: 'gpt-5-4-nano',
    name: 'GPT-5.4 Nano',
    provider: 'OpenAI',
    inputPricePer1K: 0.0002,
    outputPricePer1K: 0.00125,
    description: 'Fast and lightweight for simple tasks'
  },
  {
    id: 'gpt-5-4-pro',
    name: 'GPT-5.4 Pro',
    provider: 'OpenAI',
    inputPricePer1K: 0.03,
    outputPricePer1K: 0.18,
    description: 'Advanced reasoning and complex problem-solving'
  },

  // ANTHROPIC
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

  // GOOGLE
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

// Get unique providers from MODELS
export function getProviders() {
  const providers = [...new Set(MODELS.map(model => model.provider))];
  return providers.sort();
}

// Get models filtered by provider
export function getModelsByProvider(provider) {
  if (provider === 'All') return MODELS;
  return MODELS.filter(model => model.provider === provider);
}

// Provider logo mapping
export const PROVIDER_LOGOS = {
  'OpenAI': '/logos/openai_logo.png',
  'Anthropic': '/logos/claude_logo.png',
  'Google': '/logos/gemini_logo.png',
};

// Get first available model for a provider
export function getFirstModelForProvider(provider) {
  const models = getModelsByProvider(provider);
  return models.length > 0 ? models[0] : MODELS[0];
}