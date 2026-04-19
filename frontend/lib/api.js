const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://127.0.0.1:8000";

async function parseApiResponse(response) {
  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    const detail = payload?.detail || payload?.message || payload || "Request failed";
    const error = new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
    error.payload = payload;
    throw error;
  }

  return payload;
}

export async function uploadExcel(file) {
  const formData = new FormData();
  formData.append("file", file);

  const response = await fetch(`${API_BASE_URL}/upload`, {
    method: "POST",
    body: formData
  });

  return parseApiResponse(response);
}

export async function analyzeQuery(query, language) {
  const response = await fetch(`${API_BASE_URL}/analyze`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query, language })
  });

  return parseApiResponse(response);
}

export async function executeQuery(query, language) {
  const response = await fetch(`${API_BASE_URL}/execute`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query, language })
  });

  return parseApiResponse(response);
}
