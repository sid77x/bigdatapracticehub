"use client";

import { useMemo, useState } from "react";
import QueryEditor from "../components/QueryEditor";
import { analyzeQuery, executeQuery, uploadExcel } from "../lib/api";

const SAMPLE_QUERIES = {
  sparksql: [
    "SELECT city, COUNT(*) AS total FROM data GROUP BY city ORDER BY total DESC;",
    "SELECT age, AVG(salary) AS avg_salary FROM data GROUP BY age;"
  ],
  hiveql: [
    "SELECT department, SUM(sales) AS total_sales FROM data GROUP BY department;",
    "SELECT * FROM data WHERE age > 30 LIMIT 20;"
  ],
  piglatin: [
    "records = LOAD 'input.xlsx' USING PigStorage(',');\nfiltered = FILTER records BY age > 30;\nresult = FOREACH filtered GENERATE name, age;\nDUMP result;",
    "records = LOAD 'input.xlsx' USING PigStorage(',');\ngrouped = GROUP records BY city;\nDUMP grouped;"
  ]
};

const LANGUAGE_OPTIONS = [
  { value: "sparksql", label: "SparkSQL" },
  { value: "hiveql", label: "HiveQL" },
  { value: "piglatin", label: "Pig Latin" }
];

function parseErrorPayload(error) {
  if (error?.payload?.detail?.analysis) {
    return error.payload.detail.analysis;
  }
  return null;
}

export default function HomePage() {
  const [language, setLanguage] = useState("sparksql");
  const [query, setQuery] = useState(SAMPLE_QUERIES.sparksql[0]);
  const [schema, setSchema] = useState([]);
  const [fileName, setFileName] = useState("");
  const [statusMessage, setStatusMessage] = useState("Upload a .xlsx, .csv, or .txt file to begin.");
  const [analysis, setAnalysis] = useState(null);
  const [execution, setExecution] = useState(null);
  const [busyAction, setBusyAction] = useState("");

  const sampleList = useMemo(() => SAMPLE_QUERIES[language] || [], [language]);

  async function handleFileUpload(event) {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    try {
      setBusyAction("upload");
      const payload = await uploadExcel(file);
      setSchema(payload.schema || []);
      setFileName(file.name);
      setStatusMessage(`Loaded ${payload.row_count} rows into table data.`);
    } catch (error) {
      setStatusMessage(error.message || "Upload failed.");
    } finally {
      setBusyAction("");
      event.target.value = "";
    }
  }

  async function handleAnalyze() {
    try {
      setBusyAction("analyze");
      setExecution(null);
      const payload = await analyzeQuery(query, language);
      setAnalysis(payload);
      setStatusMessage(payload.is_valid ? "No syntax issues found." : "Found syntax issues. Check highlights and suggestions.");
    } catch (error) {
      setStatusMessage(error.message || "Analyze request failed.");
    } finally {
      setBusyAction("");
    }
  }

  async function handleExecute() {
    try {
      setBusyAction("execute");
      const payload = await executeQuery(query, language);
      setExecution(payload);
      setAnalysis(payload.analysis);
      setStatusMessage(`Execution complete. Returned ${payload.row_count} rows.`);
    } catch (error) {
      const recovered = parseErrorPayload(error);
      if (recovered) {
        setAnalysis(recovered);
      }
      setExecution(null);
      setStatusMessage(error.message || "Execution failed.");
    } finally {
      setBusyAction("");
    }
  }

  function loadSample(sample) {
    setQuery(sample);
    setAnalysis(null);
    setExecution(null);
  }

  function onLanguageChange(nextLanguage) {
    setLanguage(nextLanguage);
    setQuery(SAMPLE_QUERIES[nextLanguage][0] || "");
    setAnalysis(null);
    setExecution(null);
  }

  const errors = analysis?.errors || [];

  return (
    <main className="page">
      <div className="hero">
        <h1>Big Data Query Tutor</h1>
        <p>Practice SparkSQL, HiveQL, and Pig Latin with beginner-friendly explanations and guided fixes.</p>
      </div>

      <section className="grid">
        <aside className="card sidebar">
          <h2>Data Setup</h2>
          <label className="uploadLabel" htmlFor="data-file">
            Upload data file (.xlsx, .csv, .txt, max 10MB)
          </label>
          <input id="data-file" type="file" accept=".xlsx,.csv,.txt,text/plain,text/csv" onChange={handleFileUpload} />
          <p className="hint">{fileName ? `Loaded file: ${fileName}` : "No file uploaded yet."}</p>

          <h3>Schema</h3>
          {schema.length === 0 ? (
            <p className="hint">Schema appears here after upload.</p>
          ) : (
            <ul className="schemaList">
              {schema.map((column) => (
                <li key={column.name}>
                  <span>{column.name}</span>
                  <small>{column.type}</small>
                </li>
              ))}
            </ul>
          )}
        </aside>

        <section className="card workspace">
          <div className="toolbar">
            <label htmlFor="language">Language</label>
            <select id="language" value={language} onChange={(event) => onLanguageChange(event.target.value)}>
              {LANGUAGE_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>

            <button type="button" onClick={handleAnalyze} disabled={busyAction !== ""}>
              {busyAction === "analyze" ? "Analyzing..." : "Analyze Query"}
            </button>
            <button type="button" className="execute" onClick={handleExecute} disabled={busyAction !== ""}>
              {busyAction === "execute" ? "Executing..." : "Execute Query"}
            </button>
          </div>

          <QueryEditor value={query} onChange={setQuery} language={language} errors={errors} />

          <div className="samples">
            <h3>Sample Queries</h3>
            <div className="sampleButtons">
              {sampleList.map((sample, idx) => (
                <button key={`${language}-${idx}`} type="button" onClick={() => loadSample(sample)}>
                  Sample {idx + 1}
                </button>
              ))}
            </div>
          </div>
        </section>
      </section>

      <section className="card output">
        <h2>Output Panel</h2>
        <p className="status">{statusMessage}</p>

        <div className="outputGrid">
          <div>
            <h3>Errors</h3>
            {errors.length === 0 ? (
              <p className="hint">No errors detected yet.</p>
            ) : (
              <ul className="errorList">
                {errors.map((error, index) => (
                  <li key={`${error.code}-${index}`}>
                    <strong>{error.message}</strong>
                    <p>{error.explanation}</p>
                    <small>
                      line {error.start_line}, col {error.start_column}
                    </small>
                  </li>
                ))}
              </ul>
            )}
          </div>

          <div>
            <h3>Suggestions</h3>
            {analysis?.suggestions?.length ? (
              <ul className="suggestionList">
                {analysis.suggestions.map((tip, idx) => (
                  <li key={`tip-${idx}`}>{tip}</li>
                ))}
              </ul>
            ) : (
              <p className="hint">Suggestions appear here after analysis.</p>
            )}

            {analysis?.corrected_query ? (
              <>
                <h4>Suggested Corrected Query</h4>
                <pre>{analysis.corrected_query}</pre>
              </>
            ) : null}

            {(analysis?.translated_query || execution?.translated_query) ? (
              <>
                <h4>Translated Pig Script</h4>
                <pre>{execution?.translated_query || analysis?.translated_query}</pre>
              </>
            ) : null}
          </div>
        </div>

        <div className="results">
          <h3>Results</h3>
          {!execution ? (
            <p className="hint">Run Execute Query to see result rows.</p>
          ) : execution.rows.length === 0 ? (
            <p className="hint">Query ran successfully but returned no rows.</p>
          ) : (
            <div className="tableWrap">
              <table>
                <thead>
                  <tr>
                    {execution.columns.map((column) => (
                      <th key={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {execution.rows.map((row, rowIdx) => (
                    <tr key={`row-${rowIdx}`}>
                      {execution.columns.map((column) => (
                        <td key={`${rowIdx}-${column}`}>{String(row[column] ?? "")}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>
    </main>
  );
}
