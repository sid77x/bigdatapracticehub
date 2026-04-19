# BigData Learner Lab

Practice MapReduce, Pig, Hive, HBase, Spark SQL, and Spark ML workflows in one web app.

## What this app now does

- Keeps your script text after you click Submit Job.
- Auto-generates starter LOAD commands when you upload data files.
- Shows syntax-highlighted script editing.
- Shows execution logs and a dedicated execution output block for every job.
- Runs in no-install `practice` mode by default (no Hadoop/Spark setup needed).

## Stack

- Frontend: React + Vite
- Backend: Express + Multer + queued job runner
- Unified deployment: Vercel static frontend + Vercel serverless API (`/api/index.js`)

## Project layout

- `frontend/`: UI
- `backend/`: app logic and execution engine
- `api/index.js`: Vercel serverless entry that mounts backend app
- `vercel.json`: single-project frontend + backend deployment config

## 1. Run locally

### Prerequisites

- Node.js 20+
- npm 10+

### Setup

1. Copy `.env.example` to `.env`.
2. Install dependencies:

```bash
npm install
```

3. Start app:

```bash
npm run dev
```

### Local URLs

- Frontend: `http://localhost:5173`
- Backend API: `http://localhost:8080/api/*`

Note: Frontend calls `/api/*` and Vite proxies that to port `8080` in development.

## 2. Execution modes

### `practice` (default)

```env
EXECUTION_MODE=practice
```

No external big-data binaries required. The app parses script intent and produces data-aware output summaries.

### `simulate`

```env
EXECUTION_MODE=simulate
```

Mock execution flow for UI/API testing.

### `local` (optional advanced)

```env
EXECUTION_MODE=local
```

Uses local commands (`hadoop`, `pig`, `hive`, `hbase`, `spark-sql`, `spark-submit`) if installed on your machine.

## 3. API

- `GET /api/health`
- `GET /api/engines`
- `GET /api/jobs`
- `GET /api/jobs/:id`
- `POST /api/jobs` (`multipart/form-data`)

POST fields:

- `title`
- `engine`
- `language`
- `code` (required)
- `extraArgs`
- `dataFiles` (multiple)

## 4. Deploy everything together on Vercel (quick path)

This repository is now configured for single-project deployment on Vercel (frontend + backend together).

### A) Push to GitHub

If repo is not initialized yet:

```bash
git init
git add .
git commit -m "Initial BigData Learner platform"
git branch -M main
git remote add origin https://github.com/<your-username>/<your-repo>.git
git push -u origin main
```

If repo already exists:

```bash
git add .
git commit -m "Update platform and Vercel all-in-one deployment"
git push
```

### B) Import into Vercel

1. Open Vercel and import your GitHub repository.
2. Keep Root Directory as repo root.
3. Framework can stay auto-detected.
4. Confirm build/output are picked from `vercel.json`.
5. Add environment variables:

```env
EXECUTION_MODE=practice
MAX_FILE_SIZE_MB=100
```

6. Deploy.

### C) Optional Vercel CLI quick deploy

```bash


vercel login
vercel
vercel --prod
```

## 5. Important Vercel limitations

- Vercel filesystem is ephemeral. Uploaded files and job history are temporary.
- Long-running heavy distributed jobs are not suitable for serverless limits.
- Keep `EXECUTION_MODE=practice` on Vercel for fast, reliable behavior.

If later you need persistent jobs/results, move storage to DB/Object Storage (e.g., Postgres + S3).
