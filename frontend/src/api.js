const API_BASE = (import.meta.env.VITE_API_BASE_URL || "").trim();

function apiUrl(path) {
  return `${API_BASE}${path}`;
}

async function parseResponse(res) {
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(data.error || "Request failed");
  }
  return data;
}

export async function getEngines() {
  const res = await fetch(apiUrl("/api/engines"));
  return parseResponse(res);
}

export async function getJobs() {
  const res = await fetch(apiUrl("/api/jobs"));
  return parseResponse(res);
}

export async function submitJob(payload) {
  const res = await fetch(apiUrl("/api/jobs"), {
    method: "POST",
    body: payload
  });

  return parseResponse(res);
}
