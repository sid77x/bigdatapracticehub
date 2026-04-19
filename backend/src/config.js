import dotenv from "dotenv";
import path from "path";

dotenv.config({ path: path.resolve(process.cwd(), "../.env") });
dotenv.config();

const maxFileSizeMb = Number.parseInt(process.env.MAX_FILE_SIZE_MB || "100", 10);
const isVercelRuntime = Boolean(process.env.VERCEL || process.env.VERCEL_URL);

export const config = {
  port: Number.parseInt(process.env.PORT || "8080", 10),
  executionMode: process.env.EXECUTION_MODE || "practice",
  uploadDir: process.env.UPLOAD_DIR || (isVercelRuntime ? "/tmp/bigdata-learner/uploads" : path.resolve(process.cwd(), "uploads")),
  maxFileSizeBytes: Number.isNaN(maxFileSizeMb) ? 100 * 1024 * 1024 : maxFileSizeMb * 1024 * 1024,
  frontendOrigin: process.env.FRONTEND_ORIGIN || "http://localhost:5173",
  isVercelRuntime
};
