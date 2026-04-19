import { useEffect, useMemo, useRef, useState } from "react";
import Editor from "react-simple-code-editor";
import Prism from "prismjs";
import "prismjs/components/prism-sql";
import "prismjs/components/prism-python";
import "prismjs/components/prism-java";
import { getEngines, getJobs, submitJob } from "./api";

const THEME_STORAGE_KEY = "bigdata-learner-theme";

const statusClass = {
  queued: "badge queued",
  running: "badge running",
  completed: "badge completed",
  failed: "badge failed"
};

function escapeSingleQuotes(value) {
  return value.replace(/'/g, "\\'");
}

function buildLoadTemplate(engine, language, files) {
  if (!files.length) {
    return "";
  }

  const names = files.map((file) => escapeSingleQuotes(file.name));

  switch (engine) {
    case "pig": {
      const loadLines = names.map(
        (name, idx) => `input_${idx + 1} = LOAD '${name}' USING PigStorage(',') AS (line:chararray);`
      );
      return `${loadLines.join("\n")}\n\nDUMP input_1;`;
    }

    case "hive": {
      const tableLines = names
        .map(
          (name, idx) =>
            `LOAD DATA LOCAL INPATH '${name}' INTO TABLE input_${idx + 1};`
        )
        .join("\n");

      return `-- Update schema to match your file\nCREATE TABLE IF NOT EXISTS input_1 (col1 STRING);\n${tableLines}\n\nSELECT * FROM input_1 LIMIT 20;`;
    }

    case "sparksql": {
      const viewLines = names
        .map(
          (name, idx) =>
            `CREATE OR REPLACE TEMP VIEW input_${idx + 1} USING csv OPTIONS (path '${name}', header 'true', inferSchema 'true');`
        )
        .join("\n");

      return `${viewLines}\n\nSELECT * FROM input_1 LIMIT 20;`;
    }

    case "sparkml": {
      if (language === "python") {
        return `from pyspark.sql import SparkSession\n\nspark = SparkSession.builder.appName('BigDataLearner').getOrCreate()\ndf = spark.read.option('header', True).csv('${names[0]}')\ndf.show(20, truncate=False)`;
      }

      if (language === "scala") {
        return `val spark = org.apache.spark.sql.SparkSession.builder.appName(\"BigDataLearner\").getOrCreate()\nval df = spark.read.option(\"header\", \"true\").csv(\"${names[0]}\")\ndf.show(20, truncate = false)`;
      }

      return `// SparkML Java starter\n// Use uploaded file: ${names[0]}`;
    }

    case "hbase":
      return `# HBase shell starter\ncreate 'input_table', 'cf'\n# LOAD_FILE '${names[0]}'\nscan 'input_table'`;

    case "mapreduce": {
      if (language === "python") {
        return `# Uploaded inputs\nINPUT_FILES = [${names.map((name) => `'${name}'`).join(", ")}]\n\nfor file_name in INPUT_FILES:\n    print(\"Processing\", file_name)`;
      }

      return `// MapReduce Java jar mode\n// Uploaded data files: ${names.join(", ")}`;
    }

    default:
      return names.map((name) => `LOAD '${name}'`).join("\n");
  }
}

function getEditorLanguage(engine, language) {
  if (engine === "pig" || engine === "hive" || engine === "sparksql") {
    return "sql";
  }

  if (engine === "mapreduce" || engine === "sparkml") {
    if (language === "python") {
      return "python";
    }

    if (language === "java") {
      return "java";
    }
  }

  return "sql";
}

function deriveExecutionOutput(job) {
  if (!job) {
    return "Select a job to inspect output.";
  }

  if (job.result?.stdout?.trim()) {
    return job.result.stdout;
  }

  if (job.error?.stderr?.trim()) {
    return job.error.stderr;
  }

  if (job.error?.message) {
    return `Execution failed: ${job.error.message}`;
  }

  if (job.status === "queued") {
    return "Job is queued. Output will appear once execution starts.";
  }

  if (job.status === "running") {
    return "Job is running. Live output will appear here.";
  }

  if (job.status === "completed") {
    const command = job.result?.command || "practice-runner";
    const args = Array.isArray(job.result?.args) ? job.result.args.join(" ") : "";
    const fallback = [`Execution completed.`, `Command: ${command} ${args}`.trim()].join("\n");
    return fallback;
  }

  return "No output available for this run.";
}

function deriveOutputTables(job) {
  const tables = job?.result?.tables;
  if (!Array.isArray(tables)) {
    return [];
  }

  return tables.filter(
    (table) =>
      table &&
      typeof table.title === "string" &&
      Array.isArray(table.headers) &&
      Array.isArray(table.rows)
  );
}

function getInitialTheme() {
  if (typeof window === "undefined") {
    return "dark";
  }

  const storedTheme = window.localStorage.getItem(THEME_STORAGE_KEY);
  if (storedTheme === "light" || storedTheme === "dark") {
    return storedTheme;
  }

  return "dark";
}

export default function App() {
  const [engineLanguages, setEngineLanguages] = useState({});
  const [jobs, setJobs] = useState([]);
  const [selectedJobId, setSelectedJobId] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [theme, setTheme] = useState(getInitialTheme);

  const [form, setForm] = useState({
    title: "",
    engine: "mapreduce",
    language: "python",
    code: "",
    extraArgs: ""
  });
  const [dataFiles, setDataFiles] = useState([]);
  const autoTemplateRef = useRef("");
  const dataFileInputRef = useRef(null);

  const selectedJob = useMemo(() => jobs.find((job) => job.id === selectedJobId) || null, [jobs, selectedJobId]);
  const editorLanguage = getEditorLanguage(form.engine, form.language);
  const executionOutput = useMemo(() => deriveExecutionOutput(selectedJob), [selectedJob]);
  const outputTables = useMemo(() => deriveOutputTables(selectedJob), [selectedJob]);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  }, [theme]);

  useEffect(() => {
    async function bootstrap() {
      try {
        const engines = await getEngines();
        setEngineLanguages(engines.engineLanguages || {});

        const firstEngine = Object.keys(engines.engineLanguages || {})[0] || "mapreduce";
        const firstLanguage = engines.engineLanguages?.[firstEngine]?.[0] || "python";
        setForm((prev) => ({ ...prev, engine: firstEngine, language: firstLanguage }));
      } catch (err) {
        setError(err.message);
      }
    }

    void bootstrap();
  }, []);

  useEffect(() => {
    let stopped = false;

    async function pullJobs() {
      try {
        const response = await getJobs();
        if (!stopped) {
          setJobs(response.jobs || []);
          if (!selectedJobId && response.jobs?.length) {
            setSelectedJobId(response.jobs[0].id);
          }
        }
      } catch (err) {
        if (!stopped) {
          setError(err.message);
        }
      }
    }

    void pullJobs();
    const handle = setInterval(pullJobs, 3000);
    return () => {
      stopped = true;
      clearInterval(handle);
    };
  }, [selectedJobId]);

  const languages = engineLanguages[form.engine] || [];

  function maybeApplyLoadTemplate(engine, language, files) {
    const template = buildLoadTemplate(engine, language, files);
    if (!template) {
      return;
    }

    setForm((prev) => {
      const canOverwrite = !prev.code.trim() || prev.code === autoTemplateRef.current;
      if (!canOverwrite) {
        return prev;
      }

      autoTemplateRef.current = template;
      return {
        ...prev,
        code: template
      };
    });
  }

  function onEngineChange(engine) {
    const available = engineLanguages[engine] || [];
    const nextLanguage = available[0] || "";

    setForm((prev) => ({
      ...prev,
      engine,
      language: nextLanguage
    }));

    maybeApplyLoadTemplate(engine, nextLanguage, dataFiles);
  }

  function onSelectJob(job) {
    setSelectedJobId(job.id);

    const scriptSource = typeof job.scriptSource === "string" ? job.scriptSource : "";
    autoTemplateRef.current = scriptSource;

    setForm((prev) => ({
      ...prev,
      title: job.title || prev.title,
      engine: job.engine || prev.engine,
      language: job.language || prev.language,
      code: scriptSource || prev.code,
      extraArgs: job.params?.extraArgs || ""
    }));
  }

  async function onSubmit(event) {
    event.preventDefault();
    setLoading(true);
    setError("");

    try {
      const payload = new FormData();
      payload.append("title", form.title);
      payload.append("engine", form.engine);
      payload.append("language", form.language);
      payload.append("code", form.code);
      payload.append("extraArgs", form.extraArgs);

      for (const file of dataFiles) {
        payload.append("dataFiles", file);
      }

      const { job } = await submitJob(payload);
      setSelectedJobId(job.id);
      const refreshed = await getJobs();
      setJobs(refreshed.jobs || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  function onThemeToggle() {
    setTheme((prev) => (prev === "dark" ? "light" : "dark"));
  }

  return (
    <div className="page">
      <div className="aurora" aria-hidden="true" />
      <header className="hero">
        <div className="hero-top">
          <div>
            <p className="kicker">Data Engineering Sandbox</p>
            <h1>BigData Learner Lab</h1>
          </div>
          <button className="theme-toggle" type="button" onClick={onThemeToggle}>
            {theme === "dark" ? "Light mode" : "Dark mode"}
          </button>
        </div>
        <p>
          Practice MapReduce, Pig, Hive, HBase, Spark SQL, and Spark ML workflows with your own uploaded files.
        </p>
      </header>

      {error ? <p className="error-banner">{error}</p> : null}

      <main className="layout">
        <section className="panel form-panel">
          <h2>Submit a Job</h2>
          <form onSubmit={onSubmit}>
            <label>
              Job title
              <input
                value={form.title}
                onChange={(e) => setForm((prev) => ({ ...prev, title: e.target.value }))}
                placeholder="customer-sales-analysis"
              />
            </label>

            <label>
              Engine
              <select value={form.engine} onChange={(e) => onEngineChange(e.target.value)}>
                {Object.keys(engineLanguages).map((engine) => (
                  <option key={engine} value={engine}>
                    {engine}
                  </option>
                ))}
              </select>
            </label>

            <label>
              Language
              <select
                value={form.language}
                onChange={(e) => {
                  const nextLanguage = e.target.value;
                  setForm((prev) => ({ ...prev, language: nextLanguage }));
                  maybeApplyLoadTemplate(form.engine, nextLanguage, dataFiles);
                }}
              >
                {languages.map((language) => (
                  <option key={language} value={language}>
                    {language}
                  </option>
                ))}
              </select>
            </label>

            <label>
              Script text
              <Editor
                value={form.code}
                onValueChange={(value) => {
                  setForm((prev) => ({ ...prev, code: value }));
                }}
                highlight={(code) =>
                  Prism.highlight(code, Prism.languages[editorLanguage] || Prism.languages.sql, editorLanguage)
                }
                padding={12}
                className="code-editor"
                textareaClassName="code-editor-input"
                preClassName="code-editor-preview"
                placeholder="Write your Pig/Hive/Spark script here"
              />
            </label>

            <label>
              Data files
              <input
                ref={dataFileInputRef}
                className="hidden-file-input"
                type="file"
                multiple
                onChange={(e) => {
                  const files = Array.from(e.target.files || []);
                  setDataFiles(files);
                  maybeApplyLoadTemplate(form.engine, form.language, files);
                }}
              />
              <button
                type="button"
                className="file-upload-btn"
                onClick={() => dataFileInputRef.current?.click()}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path d="M7 18a4 4 0 0 1-.7-7.94A6 6 0 0 1 18 8a4 4 0 0 1 1 7.87V16a3 3 0 0 1-3 3H8a3 3 0 0 1-3-3v-.13A3.98 3.98 0 0 1 7 18Zm5-9-3 3h2v4h2v-4h2l-3-3Z" />
                </svg>
                Upload data files
              </button>
              <span className="file-list-text">
                {dataFiles.length ? dataFiles.map((file) => file.name).join(", ") : "No files selected"}
              </span>
            </label>

            <label>
              Extra command args (optional)
              <input
                value={form.extraArgs}
                onChange={(e) => setForm((prev) => ({ ...prev, extraArgs: e.target.value }))}
                placeholder="--master local[*]"
              />
            </label>

            <button disabled={loading} type="submit">
              {loading ? "Submitting..." : "Run Job"}
            </button>
          </form>
        </section>

        <section className="panel jobs-panel">
          <div className="section-heading">
            <h2>Jobs</h2>
            <p>{jobs.length} submitted</p>
          </div>

          <div className="job-list">
            {jobs.map((job) => (
              <button
                key={job.id}
                className={`job-card ${job.id === selectedJobId ? "active" : ""}`}
                onClick={() => onSelectJob(job)}
                type="button"
              >
                <div>
                  <h3>{job.title}</h3>
                  <p>
                    {job.engine} / {job.language}
                  </p>
                </div>
                <span className={statusClass[job.status] || "badge"}>{job.status}</span>
              </button>
            ))}

            {!jobs.length ? <p className="empty-state">No jobs yet. Submit your first script.</p> : null}
          </div>
        </section>

        <section className="panel logs-panel">
          <h2>Execution Logs</h2>
          {selectedJob ? (
            <>
              <div className="meta-grid">
                <div>
                  <p className="meta-label">Job ID</p>
                  <p>{selectedJob.id}</p>
                </div>
                <div>
                  <p className="meta-label">Created</p>
                  <p>{new Date(selectedJob.createdAt).toLocaleString()}</p>
                </div>
                <div>
                  <p className="meta-label">Status</p>
                  <p className={statusClass[selectedJob.status] || "badge"}>{selectedJob.status}</p>
                </div>
              </div>

              <pre className="logs">{selectedJob.logs?.join("\n") || "No logs yet"}</pre>

              <h3 className="output-heading">Execution Output</h3>
              <pre className="logs">{executionOutput}</pre>

              {outputTables.length ? (
                <div className="output-tables">
                  {outputTables.map((table, tableIdx) => (
                    <div key={`${table.title}-${tableIdx}`} className="output-table-card">
                      <p className="output-table-title">{table.title}</p>
                      <div className="output-table-wrap">
                        <table className="output-table">
                          <thead>
                            <tr>
                              {table.headers.map((header, headerIdx) => (
                                <th key={`${table.title}-h-${headerIdx}`}>{header}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {table.rows.map((row, rowIdx) => (
                              <tr key={`${table.title}-r-${rowIdx}`}>
                                {row.map((cell, cellIdx) => (
                                  <td key={`${table.title}-c-${rowIdx}-${cellIdx}`}>{cell}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ))}
                </div>
              ) : null}

              {selectedJob.error ? (
                <pre className="error-block">{JSON.stringify(selectedJob.error, null, 2)}</pre>
              ) : null}
            </>
          ) : (
            <p className="empty-state">Select a job to inspect logs.</p>
          )}
        </section>
      </main>

      <footer className="app-footer">
        <p>
          Made with <span className="heart" aria-label="heart">❤</span> by Siddhant
        </p>
        <a
          className="github-link"
          href="https://github.com/sid77x"
          target="_blank"
          rel="noreferrer"
          aria-label="Siddhant GitHub profile"
          title="github.com/sid77x"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M12 0.297C5.373 0.297 0 5.67 0 12.297c0 5.303 3.438 9.8 8.205 11.387 0.6 0.113 0.82-0.258 0.82-0.577 0-0.285-0.01-1.039-0.016-2.04-3.338 0.724-4.042-1.61-4.042-1.61-0.546-1.385-1.333-1.754-1.333-1.754-1.089-0.744 0.083-0.729 0.083-0.729 1.204 0.085 1.838 1.237 1.838 1.237 1.07 1.833 2.809 1.303 3.495 0.996 0.108-0.776 0.417-1.305 0.76-1.605-2.665-0.303-5.466-1.332-5.466-5.93 0-1.31 0.468-2.381 1.235-3.221-0.124-0.303-0.535-1.524 0.117-3.176 0 0 1.008-0.322 3.301 1.23 0.957-0.266 1.984-0.399 3.003-0.404 1.018 0.005 2.046 0.138 3.005 0.404 2.291-1.552 3.297-1.23 3.297-1.23 0.653 1.653 0.243 2.874 0.119 3.176 0.77 0.84 1.233 1.911 1.233 3.221 0 4.609-2.807 5.624-5.479 5.921 0.43 0.371 0.814 1.102 0.814 2.222 0 1.604-0.015 2.896-0.015 3.286 0 0.321 0.216 0.694 0.825 0.576C20.565 22.092 24 17.595 24 12.297 24 5.67 18.627 0.297 12 0.297z" />
          </svg>
        </a>
      </footer>
    </div>
  );
}
