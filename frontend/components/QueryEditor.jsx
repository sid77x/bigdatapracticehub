"use client";

import { useEffect, useRef } from "react";
import Editor, { useMonaco } from "@monaco-editor/react";

const SQL_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "GROUP BY",
  "ORDER BY",
  "HAVING",
  "LIMIT",
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX"
];

const PIG_KEYWORDS = [
  "LOAD",
  "USING",
  "AS",
  "FILTER",
  "BY",
  "FOREACH",
  "GENERATE",
  "GROUP",
  "DUMP"
];

export default function QueryEditor({ value, onChange, language, errors }) {
  const monaco = useMonaco();
  const editorRef = useRef(null);
  const modelRef = useRef(null);
  const providersRegisteredRef = useRef(false);

  useEffect(() => {
    if (!monaco || providersRegisteredRef.current) {
      return;
    }

    providersRegisteredRef.current = true;

    monaco.languages.registerCompletionItemProvider("sql", {
      provideCompletionItems: () => {
        const suggestions = [...SQL_KEYWORDS, ...PIG_KEYWORDS].map((keyword, idx) => ({
          label: keyword,
          kind: monaco.languages.CompletionItemKind.Keyword,
          insertText: keyword,
          sortText: `a${idx}`
        }));

        return { suggestions };
      }
    });
  }, [monaco]);

  useEffect(() => {
    if (!monaco || !modelRef.current) {
      return;
    }

    const markers = (errors || []).map((error) => ({
      severity: monaco.MarkerSeverity.Error,
      message: error.message || error.explanation || "Syntax error",
      startLineNumber: error.start_line || 1,
      startColumn: error.start_column || 1,
      endLineNumber: error.end_line || error.start_line || 1,
      endColumn: error.end_column || (error.start_column || 1) + 1
    }));

    monaco.editor.setModelMarkers(modelRef.current, "query-analysis", markers);
  }, [errors, monaco]);

  function handleMount(editor) {
    editorRef.current = editor;
    modelRef.current = editor.getModel();
  }

  return (
    <Editor
      height="360px"
      defaultLanguage={language === "piglatin" ? "sql" : "sql"}
      language={language === "piglatin" ? "sql" : "sql"}
      value={value}
      onChange={(next) => onChange(next || "")}
      onMount={handleMount}
      options={{
        minimap: { enabled: false },
        fontSize: 14,
        lineNumbers: "on",
        roundedSelection: true,
        automaticLayout: true,
        tabSize: 2,
        theme: "vs-dark",
        suggestOnTriggerCharacters: true,
        quickSuggestions: true,
        wordWrap: "on"
      }}
    />
  );
}
