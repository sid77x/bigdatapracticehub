import cors from "cors";
import express from "express";
import fs from "fs/promises";
import multer from "multer";
import path from "path";
import pino from "pino";
import pinoHttp from "pino-http";
import { v4 as uuidv4 } from "uuid";
import { config } from "./config.js";
import { detectScriptExt, engineLanguages, isValidEngineLanguage } from "./engines/index.js";
import { createJobRunner } from "./runner.js";
import { JobStore } from "./storage.js";

const logger = pino({ level: "info" });
let appPromise = null;
const validationProfileVersion = "practice-validator-v2-2026-04-19";

function safeName(filename) {
  return filename.replace(/[^a-zA-Z0-9._-]/g, "_");
}

function normalizeNewlines(value = "") {
  return value.replace(/\r\n/g, "\n");
}

function resolveStorePath() {
  if (config.isVercelRuntime) {
    return "/tmp/bigdata-learner/jobs.json";
  }

  return path.resolve(process.cwd(), "data", "jobs.json");
}

async function resolveWritableUploadDir(preferredDir) {
  try {
    await fs.mkdir(preferredDir, { recursive: true });
    return preferredDir;
  } catch (error) {
    const fallbackDir = "/tmp/bigdata-learner/uploads";

    if (preferredDir === fallbackDir) {
      throw error;
    }

    logger.warn(
      {
        preferredDir,
        fallbackDir,
        error: error.message
      },
      "Configured upload directory failed, falling back to /tmp"
    );

    await fs.mkdir(fallbackDir, { recursive: true });
    return fallbackDir;
  }
}

async function hydrateJobScriptSource(job) {
  if (typeof job.scriptSource === "string") {
    return job;
  }

  if (!job.scriptPath) {
    return { ...job, scriptSource: "" };
  }

  try {
    const scriptSource = await fs.readFile(job.scriptPath, "utf8");
    return { ...job, scriptSource };
  } catch {
    return { ...job, scriptSource: "" };
  }
}

async function createApp() {
  const app = express();

  app.use(pinoHttp({ logger }));
  app.use(cors({ origin: config.frontendOrigin, credentials: false }));
  app.use(express.json({ limit: "1mb" }));

  const upload = multer({
    storage: multer.memoryStorage(),
    limits: {
      fileSize: config.maxFileSizeBytes
    }
  });

  const store = new JobStore(resolveStorePath());
  await store.init();

  const uploadDir = await resolveWritableUploadDir(config.uploadDir);

  const runner = createJobRunner({
    store,
    executionMode: config.executionMode,
    logger
  });

  app.get("/api/health", (_req, res) => {
    res.json({
      status: "ok",
      executionMode: config.executionMode,
      runner: runner.stats(),
      runtime: config.isVercelRuntime ? "vercel" : "node",
      uploadDir,
      validationProfileVersion
    });
  });

  app.get("/api/engines", (_req, res) => {
    res.json({ engineLanguages });
  });

  app.get("/api/jobs", async (_req, res) => {
    const jobs = await Promise.all(store.list().map((job) => hydrateJobScriptSource(job)));
    res.json({ jobs });
  });

  app.get("/api/jobs/:id", async (req, res) => {
    const job = store.get(req.params.id);
    if (!job) {
      res.status(404).json({ error: "Job not found" });
      return;
    }

    const hydrated = await hydrateJobScriptSource(job);
    res.json({ job: hydrated });
  });

  app.post(
    "/api/jobs",
    upload.fields([{ name: "dataFiles", maxCount: 20 }]),
    async (req, res) => {
      try {
        const engine = String(req.body.engine || "").trim();
        const language = String(req.body.language || "").trim();
        const code = normalizeNewlines(String(req.body.code || ""));
        const title = String(req.body.title || "").trim() || `${engine}-${Date.now()}`;
        const className = String(req.body.className || "").trim();
        const extraArgs = String(req.body.extraArgs || "").trim();

        if (!isValidEngineLanguage(engine, language)) {
          res.status(400).json({ error: "Invalid engine or language combination" });
          return;
        }

        if (!code.trim()) {
          res.status(400).json({ error: "Provide script code" });
          return;
        }

        const files = req.files || {};
        const dataFiles = files.dataFiles || [];

        const id = uuidv4();
        const workingDirectory = path.join(uploadDir, id);
        await fs.mkdir(workingDirectory, { recursive: true });

        const scriptName = `script.${detectScriptExt(engine, language)}`;
        const scriptPath = path.join(workingDirectory, scriptName);
        await fs.writeFile(scriptPath, code, "utf8");

        const persistedDataFiles = [];
        for (const file of dataFiles) {
          const dataName = safeName(file.originalname || "data.bin");
          const dataPath = path.join(workingDirectory, dataName);
          await fs.writeFile(dataPath, file.buffer);
          persistedDataFiles.push({
            name: dataName,
            path: dataPath,
            size: file.size
          });
        }

        const job = {
          id,
          title,
          status: "queued",
          engine,
          language,
          createdAt: new Date().toISOString(),
          startedAt: null,
          endedAt: null,
          workingDirectory,
          scriptName,
          scriptPath,
          scriptSource: code,
          dataFiles: persistedDataFiles.map((f) => ({ name: f.name, size: f.size })),
          dataFilePaths: persistedDataFiles.map((f) => f.path),
          params: {
            className,
            extraArgs
          },
          result: null,
          error: null,
          logs: []
        };

        await store.create(job);
        runner.enqueue(job.id);

        res.status(201).json({ job });
      } catch (error) {
        req.log.error({ err: error }, "Failed to submit job");
        res.status(500).json({ error: error.message || "Failed to submit job" });
      }
    }
  );

  return app;
}

export async function getApp() {
  if (!appPromise) {
    appPromise = createApp();
  }

  return appPromise;
}
