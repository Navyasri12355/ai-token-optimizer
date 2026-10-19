// In production (Vercel), calls go through /api/proxy which forwards to Azure.
// In local dev, falls back to direct Azure URL via VITE_API_URL.
const IS_PROD = import.meta.env.PROD;
const API_BASE = IS_PROD ? '' : (import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000');

export async function analyzePrompt(prompt) {
  try {
    const url = IS_PROD
      ? `/api/predict?prompt=${encodeURIComponent(prompt)}`
      : `${API_BASE}/predict?prompt=${encodeURIComponent(prompt)}`;

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('API Error:', error);
    throw error;
  }
}

export async function healthCheck() {
  try {
    const url = IS_PROD ? `/api/health` : `${API_BASE}/health`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('Health check failed:', error);
    throw error;
  }
}
