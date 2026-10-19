const AZURE_API = 'https://token-optimizer-api.braverock-2f6b3f25.centralindia.azurecontainerapps.io';

export default async function handler(req, res) {
  const { path = '', ...query } = req.query;

  // Build the upstream URL preserving the path and query params
  const queryString = new URLSearchParams(query).toString();
  const upstreamUrl = `${AZURE_API}/${path}${queryString ? '?' + queryString : ''}`;

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
