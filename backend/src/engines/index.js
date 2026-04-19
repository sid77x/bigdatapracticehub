import fs from "fs/promises";
import { spawn } from "child_process";
import path from "path";

export const engineLanguages = {
  mapreduce: ["python", "java"],
  pig: ["pig-latin"],
  hive: ["hiveql"],
  hbase: ["hbase-shell"],
  sparksql: ["sql"],
  sparkml: ["python", "scala", "java"]
};

const scriptExtByEngineAndLanguage = {
  mapreduce: {
    python: "py",
    java: "jar"
  },
  pig: {
    "pig-latin": "pig"
  },
  hive: {
    hiveql: "hql"
  },
  hbase: {
    "hbase-shell": "hbase"
  },
  sparksql: {
    sql: "sql"
  },
  sparkml: {
    python: "py",
    scala: "scala",
    java: "java"
  }
};

export function isValidEngineLanguage(engine, language) {
  return Boolean(engineLanguages[engine]?.includes(language));
}

export function detectScriptExt(engine, language) {
  return scriptExtByEngineAndLanguage[engine]?.[language] || "txt";
}

function parseDelimitedRows(text) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (!lines.length) {
    return {
      delimiter: ",",
      headers: [],
      rows: []
    };
  }

  const delimiter = lines[0].includes("\t") ? "\t" : ",";
  const headers = lines[0].split(delimiter).map((part) => part.trim());
  const rows = lines.slice(1).map((line) => line.split(delimiter).map((part) => part.trim()));

  return {
    delimiter,
    headers,
    rows
  };
}

async function profileDataFile(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  const parsed = parseDelimitedRows(raw);

  return {
    name: path.basename(filePath),
    filePath,
    headers: parsed.headers,
    rows: parsed.rows,
    rowCount: parsed.rows.length,
    columnCount: parsed.headers.length,
    preview: parsed.rows.slice(0, 5)
  };
}

function extractQuotedFileNames(script) {
  const matches = script.matchAll(/["']([^"']+\.[^"']+)["']/g);
  return Array.from(matches, (match) => path.basename(match[1]));
}

function topWords(profiles) {
  const frequency = new Map();

  for (const profile of profiles) {
    for (const row of profile.rows) {
      const line = row.join(" ");
      const words = line
        .toLowerCase()
        .replace(/[^a-z0-9\s]/g, " ")
        .split(/\s+/)
        .filter((word) => word.length >= 3);

      for (const word of words) {
        frequency.set(word, (frequency.get(word) || 0) + 1);
      }
    }
  }

  return [...frequency.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([word, count]) => `${word}:${count}`);
}

function numericColumnSummaries(profile) {
  const stats = [];

  for (let colIdx = 0; colIdx < profile.columnCount; colIdx += 1) {
    const values = profile.rows
      .map((row) => Number.parseFloat(row[colIdx]))
      .filter((value) => Number.isFinite(value));

    if (values.length === 0) {
      continue;
    }

    const sum = values.reduce((acc, value) => acc + value, 0);
    const avg = sum / values.length;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const columnName = profile.headers[colIdx] || `col_${colIdx + 1}`;

    stats.push({
      columnName,
      count: values.length,
      min,
      max,
      avg
    });
  }

  return stats.slice(0, 6);
}

function buildPreviewTables(profiles) {
  return profiles
    .filter((profile) => profile.preview.length > 0)
    .map((profile) => {
      const maxColumns = profile.preview.reduce((max, row) => Math.max(max, row.length), 0);
      const headers =
        profile.headers.length > 0
          ? profile.headers
          : Array.from({ length: maxColumns }, (_, idx) => `col_${idx + 1}`);

      const rows = profile.preview.map((row) => {
        if (row.length >= headers.length) {
          return row;
        }

        return [...row, ...Array.from({ length: headers.length - row.length }, () => "")];
      });

      return {
        title: `${profile.name} preview`,
        headers,
        rows
      };
    });
}

async function executePracticeJob(job, onLog) {
  const script = await fs.readFile(job.scriptPath, "utf8").catch(() => "");
  const profiles = await Promise.all(job.dataFilePaths.map((filePath) => profileDataFile(filePath)));

  onLog(`PRACTICE MODE: Running ${job.engine}/${job.language} without external Hadoop/Spark binaries.`);

  if (!profiles.length) {
    onLog("No uploaded data files were found. Execution used script-only analysis.");
  }

  for (const profile of profiles) {
    onLog(
      `Loaded ${profile.name}: ${profile.rowCount} rows, ${profile.columnCount} columns, headers=[${profile.headers.join(", ")}]`
    );
  }

  const referencedFiles = extractQuotedFileNames(script);
  const uploadedNames = new Set(profiles.map((profile) => profile.name));
  const missingReferences = referencedFiles.filter((name) => !uploadedNames.has(name));

  if (referencedFiles.length) {
    onLog(`Script references files: ${referencedFiles.join(", ")}`);
  }

  if (missingReferences.length) {
    onLog(`Warning: Referenced files not uploaded: ${missingReferences.join(", ")}`);
  }

  let summary = "";
  const tables = buildPreviewTables(profiles);

  switch (job.engine) {
    case "mapreduce": {
      const words = topWords(profiles);
      summary = [
        "MapReduce practice summary",
        `- Input files: ${profiles.length}`,
        `- Total rows: ${profiles.reduce((acc, profile) => acc + profile.rowCount, 0)}`,
        `- Top words: ${words.join(", ") || "N/A"}`
      ].join("\n");
      break;
    }

    case "sparkml": {
      const firstProfile = profiles[0];
      const stats = firstProfile ? numericColumnSummaries(firstProfile) : [];
      summary = [
        "Spark ML practice summary",
        `- Input files: ${profiles.length}`,
        ...stats.map(
          (stat) =>
            `- ${stat.columnName}: count=${stat.count}, min=${stat.min.toFixed(4)}, max=${stat.max.toFixed(
              4
            )}, avg=${stat.avg.toFixed(4)}`
        ),
        stats.length ? "" : "- No numeric columns detected in uploaded data"
      ].join("\n");
      break;
    }

    case "hbase": {
      const createCount = (script.match(/\bcreate\b/gi) || []).length;
      const putCount = (script.match(/\bput\b/gi) || []).length;
      const getCount = (script.match(/\bget\b/gi) || []).length;
      const scanCount = (script.match(/\bscan\b/gi) || []).length;
      summary = [
        "HBase practice summary",
        `- create commands: ${createCount}`,
        `- put commands: ${putCount}`,
        `- get commands: ${getCount}`,
        `- scan commands: ${scanCount}`,
        "- Command syntax parsed in no-install practice mode"
      ].join("\n");
      break;
    }

    case "pig":
    case "hive":
    case "sparksql": {
      const operationCount = {
        load: (script.match(/\bload\b/gi) || []).length,
        filter: (script.match(/\bfilter\b/gi) || []).length,
        group: (script.match(/\bgroup\b/gi) || []).length,
        join: (script.match(/\bjoin\b/gi) || []).length,
        select: (script.match(/\bselect\b/gi) || []).length
      };

      summary = [
        `${job.engine.toUpperCase()} practice summary`,
        `- Input files: ${profiles.length}`,
        `- LOAD ops: ${operationCount.load}`,
        `- SELECT ops: ${operationCount.select}`,
        `- FILTER ops: ${operationCount.filter}`,
        `- GROUP ops: ${operationCount.group}`,
        `- JOIN ops: ${operationCount.join}`,
        "- Script parsed and linked to uploaded files"
      ].join("\n");
      break;
    }

    default:
      summary = "Practice execution completed.";
  }

  onLog("Practice execution completed.");

  return {
    exitCode: 0,
    command: "practice-runner",
    args: [job.engine, job.language],
    stdout: summary,
    stderr: "",
    tables
  };
}

function tokenizeArgs(value = "") {
  const tokenRegex = /(?:[^\s\"]+|\"[^\"]*\")+/g;
  const matches = value.match(tokenRegex) || [];
  return matches.map((segment) => segment.replace(/^\"|\"$/g, ""));
}

function buildExecutionPlan(job) {
  const extraArgs = tokenizeArgs(job.params?.extraArgs || "");

  switch (job.engine) {
    case "mapreduce": {
      if (job.language === "python") {
        return {
          command: "python",
          args: [job.scriptPath, ...job.dataFilePaths, ...extraArgs]
        };
      }

      return {
        command: "hadoop",
        args: ["jar", job.scriptPath, ...extraArgs]
      };
    }
    case "pig":
      return { command: "pig", args: ["-x", "local", job.scriptPath, ...extraArgs] };
    case "hive":
      return { command: "hive", args: ["-f", job.scriptPath, ...extraArgs] };
    case "hbase":
      return { command: "hbase", args: ["shell", job.scriptPath, ...extraArgs] };
    case "sparksql":
      return { command: "spark-sql", args: ["-f", job.scriptPath, ...extraArgs] };
    case "sparkml":
      return { command: "spark-submit", args: [job.scriptPath, ...job.dataFilePaths, ...extraArgs] };
    default:
      throw new Error(`Unsupported engine: ${job.engine}`);
  }
}

export async function executeJob(job, mode, onLog) {
  if (mode === "practice") {
    return executePracticeJob(job, onLog);
  }

  const plan = buildExecutionPlan(job);

  if (mode === "simulate") {
    onLog(`SIMULATION MODE: ${plan.command} ${plan.args.join(" ")}`);
    onLog("Simulating workload...");
    await new Promise((resolve) => setTimeout(resolve, 1500));
    onLog("Simulation completed.");

    return {
      exitCode: 0,
      command: plan.command,
      args: plan.args,
      stdout: "Simulated job output",
      stderr: ""
    };
  }

  return new Promise((resolve, reject) => {
    const child = spawn(plan.command, plan.args, {
      cwd: job.workingDirectory,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"]
    });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      stdout += text;
      onLog(text.trimEnd());
    });

    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      stderr += text;
      onLog(text.trimEnd());
    });

    child.on("error", (error) => {
      reject(error);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        const err = new Error(`Command failed with exit code ${code}`);
        err.stdout = stdout;
        err.stderr = stderr;
        err.code = code;
        reject(err);
        return;
      }

      resolve({
        exitCode: code,
        command: plan.command,
        args: plan.args,
        stdout,
        stderr
      });
    });
  });
}
