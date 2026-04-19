import dotenv from "dotenv";
import path from "path";

dotenv.config({ path: path.resolve(process.cwd(), "../.env") });
dotenv.config();

const maxFileSizeMb = Number.parseInt(process.env.MAX_FILE_SIZE_MB || "100", 10);
const isVercelRuntime = Boolean(process.env.VERCEL || process.env.VERCEL_URL);
const envUploadDir = (process.env.UPLOAD_DIR || "").trim();

function resolveUploadDir() {
  if (isVercelRuntime) {
    // Vercel functions can only write under /tmp.
    if (envUploadDir.startsWith("/tmp")) {
      return envUploadDir;
    }

    return "/tmp/bigdata-learner/uploads";
  }

  if (!envUploadDir) {
    return path.resolve(process.cwd(), "uploads");
  }

  return path.isAbsolute(envUploadDir) ? envUploadDir : path.resolve(process.cwd(), envUploadDir);
}

export const config = {
  port: Number.parseInt(process.env.PORT || "8080", 10),
  executionMode: process.env.EXECUTION_MODE || "practice",
  uploadDir: resolveUploadDir(),
  maxFileSizeBytes: Number.isNaN(maxFileSizeMb) ? 100 * 1024 * 1024 : maxFileSizeMb * 1024 * 1024,
  frontendOrigin: process.env.FRONTEND_ORIGIN || "http://localhost:5173",
  isVercelRuntime
};
