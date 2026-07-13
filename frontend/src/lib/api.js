const API_URL = import.meta.env.VITE_API_URL || 'http://127.0.0.1:8000';

export async function analyzePrompt(prompt) {
  try {
    const response = await fetch(`${API_URL}/predict?prompt=${encodeURIComponent(prompt)}`, {
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
    const response = await fetch(`${API_URL}/health`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('Health check failed:', error);
    throw error;
  }
}
