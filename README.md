# Big Data Query Tutor

Big Data Query Tutor is an educational full-stack app for learning SparkSQL, HiveQL, and Pig Latin.
It focuses on helping beginners detect syntax issues, understand them, and fix them quickly.

## Stack

- Frontend: Next.js + Monaco Editor
- Backend: FastAPI + Pandas + DuckDB
- Storage: In-memory only (no persistence)

## Features

- Upload tabular files (.xlsx, .csv, .txt, max 10MB)
- Converts uploaded data into in-memory table `data`
- Language selector: SparkSQL, HiveQL, Pig Latin
- Interactive code editor with inline error markers
- Analyzer returns:
  - Syntax errors
  - Beginner-friendly explanations
  - Suggestions and hints
  - Corrected query (when possible)
- SQL execution for valid SparkSQL/HiveQL via DuckDB
- Pig Latin syntax validation + SQL translation preview
- Schema sidebar and beginner sample queries

## Project Layout

- frontend: Next.js app
- backend: FastAPI app

## Backend Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r backend/requirements.txt
```

3. Run API server:

```bash
uvicorn app.main:app --reload --app-dir backend
```

API endpoints:

- POST /upload (accepts .xlsx, .csv, .txt)
- POST /analyze
- POST /execute
- GET /health

## Frontend Setup

1. Install dependencies:

```bash
cd frontend
npm install
```

2. Optional environment variable:

```bash
# frontend/.env.local
NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000
```

3. Run frontend:

```bash
npm run dev
```

Open http://localhost:3000

## Notes

- No persistent storage is used.
- Uploaded data and schema are kept only in backend memory.
- Restarting the backend clears uploaded data.
