export const API_BASE = (import.meta.env.VITE_API_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
const API_ROOT = `${API_BASE}/api`;

async function request(path, options = {}) {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), 12000);
  let response;
  try {
    response = await fetch(`${API_ROOT}${path}`, {
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
      signal: controller.signal,
      ...options,
    });
  } catch (error) {
    window.clearTimeout(timeoutId);
    if (error.name === "AbortError") {
      throw new Error(`Request timed out. Confirm the Django server is running on ${API_BASE}.`);
    }
    throw new Error(`Unable to reach backend. Confirm the Django server is running on ${API_BASE}.`);
  }
  window.clearTimeout(timeoutId);

  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();

  if (!response.ok) {
    if (typeof payload === "string") {
      throw new Error(payload || "Request failed");
    }
    throw new Error(payload?.detail || payload?.error || "Request failed");
  }

  return payload;
}

export const api = {
  baseUrl: API_BASE,
  apiRoot: API_ROOT,
  healthCheck() {
    return fetch(`${API_BASE}/`, { method: "GET" }).then(async (response) => {
      if (!response.ok) {
        throw new Error("Backend unavailable");
      }
      return response.json();
    });
  },
  createRoom(payload) {
    return request("/create-room", { method: "POST", body: JSON.stringify(payload) });
  },
  joinRoom(roomCode, listenerId) {

  return request("/join-room", {
    method: "POST",
    body: JSON.stringify({
      room_code: roomCode,
      listener_id: listenerId
    })
  });

},

  getRoomState(roomCode) {
    return request(`/room-state/${roomCode}`);
  },
  async checkRoomCode(roomCode) {
    const normalized = (roomCode || "").trim().toUpperCase();
    if (!normalized) {
      return { available: false, reason: "missing" };
    }

    try {
      await this.getRoomState(normalized);
      return { available: false, reason: "taken" };
    } catch (error) {
      if (String(error.message || "").includes("Not found")) {
        return { available: true, reason: "available" };
      }
      if (String(error.message || "").includes("No Room matches")) {
        return { available: true, reason: "available" };
      }
      throw error;
    }
  },
  updateRoomState(roomCode, payload) {
    return request(`/room-state/${roomCode}/update`, { method: "POST", body: JSON.stringify(payload) });
  },
};
