// Update this to your Render backend URL
const BACKEND_API = process.env.BACKEND_API_URL || 'https://ai-token-optimizer-1-o481.onrender.com';

export default async function handler(req, res) {
  const { path = '', ...query } = req.query;

  // Build the upstream URL preserving the path and query params
  const queryString = new URLSearchParams(query).toString();
  const upstreamUrl = `${BACKEND_API}/${path}${queryString ? '?' + queryString : ''}`;

  try {
    const response = await fetch(upstreamUrl, {
      method: req.method,
      headers: {
        'Content-Type': 'application/json',
      },
      ...(req.method !== 'GET' && req.body
        ? { body: JSON.stringify(req.body) }
        : {}),
    });

    const data = await response.json();
    res.status(response.status).json(data);
  } catch (err) {
    res.status(502).json({ error: 'Upstream request failed', detail: String(err) });
  }
}
