import fs from "fs/promises";
import path from "path";

export class JobStore {
  constructor(filePath) {
    this.filePath = filePath;
    this.jobs = new Map();
    this.persistQueue = Promise.resolve();
  }

  async init() {
    await fs.mkdir(path.dirname(this.filePath), { recursive: true });
    try {
      const raw = await fs.readFile(this.filePath, "utf8");
      const parsed = JSON.parse(raw);
      for (const job of parsed.jobs || []) {
        this.jobs.set(job.id, job);
      }
    } catch (err) {
      if (err.code === "ENOENT") {
        await this.#persist();
      } else {
        throw err;
      }
    }
  }

  list() {
    return Array.from(this.jobs.values()).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  }

  get(id) {
    return this.jobs.get(id) || null;
  }

  async create(job) {
    this.jobs.set(job.id, job);
    await this.#persist();
    return job;
  }

  async update(id, patch) {
    const existing = this.jobs.get(id);
    if (!existing) {
      return null;
    }

    const updated = { ...existing, ...patch };
    this.jobs.set(id, updated);
    await this.#persist();
    return updated;
  }

  async appendLog(id, message) {
    const existing = this.jobs.get(id);
    if (!existing) {
      return null;
    }

    const logs = Array.isArray(existing.logs) ? [...existing.logs, message] : [message];
    const updated = { ...existing, logs };
    this.jobs.set(id, updated);
    await this.#persist();
    return updated;
  }

  async #persist() {
    const payload = JSON.stringify({ jobs: this.list() }, null, 2);
    this.persistQueue = this.persistQueue.then(() => fs.writeFile(this.filePath, payload, "utf8"));
    return this.persistQueue;
  }
}
