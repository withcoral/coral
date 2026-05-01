import { useState, useEffect, useRef, useCallback, type CSSProperties } from "react";
import { createHighlighter, createCssVariablesTheme, type Highlighter } from "shiki";

const cssVarTheme = createCssVariablesTheme({
  name: "css-variables",
  variablePrefix: "--shiki-",
  variableDefaults: {
    foreground: "#d4d4d4",
    background: "transparent",
  },
});

let highlighterPromise: Promise<Highlighter> | null = null;

function getHighlighter(): Promise<Highlighter> {
  if (!highlighterPromise) {
    highlighterPromise = createHighlighter({
      themes: [cssVarTheme],
      langs: ["sql"],
    });
  }
  return highlighterPromise;
}

interface ShikiEditorProps {
  value: string;
  onChange: (value: string) => void;
  language?: string;
  className?: string;
  disabled?: boolean;
  placeholder?: string;
  style?: CSSProperties;
}

const sharedStyles: CSSProperties = {
  fontFamily: "'Gustan Mono', monospace",
  fontSize: 12,
  lineHeight: "18px",
  padding: "8px 12px",
  margin: 0,
  whiteSpace: "pre-wrap",
  wordWrap: "break-word",
  overflowWrap: "break-word",
  border: "none",
  outline: "none",
};

export function ShikiEditor({
  value,
  onChange,
  language = "sql",
  className,
  disabled = false,
  placeholder,
  style,
}: ShikiEditorProps) {
  const [highlightedHtml, setHighlightedHtml] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const preRef = useRef<HTMLPreElement>(null);

  const handleScroll = useCallback(() => {
    const textarea = textareaRef.current;
    const pre = preRef.current;
    if (textarea && pre) {
      pre.scrollTop = textarea.scrollTop;
      pre.scrollLeft = textarea.scrollLeft;
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    getHighlighter().then((highlighter) => {
      if (cancelled) return;
      const html = highlighter.codeToHtml(value || " ", {
        lang: language,
        theme: "css-variables",
      });
      setHighlightedHtml(html);
    });
    return () => {
      cancelled = true;
    };
  }, [value, language]);

  return (
    <div className={className} style={{ position: "relative", overflow: "hidden", ...style }}>
      <pre
        ref={preRef}
        style={{ ...sharedStyles, position: "absolute", inset: 0, overflow: "auto", pointerEvents: "none", background: "transparent" }}
        dangerouslySetInnerHTML={{
          __html: highlightedHtml
            .replace(/^<pre[^>]*><code[^>]*>/, "")
            .replace(/<\/code><\/pre>$/, ""),
        }}
      />
      {!value && placeholder ? (
        <div style={{ ...sharedStyles, position: "absolute", inset: 0, color: "#6b7280", pointerEvents: "none", zIndex: 0 }}>
          {placeholder}
        </div>
      ) : null}
      <textarea
        ref={textareaRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onScroll={handleScroll}
        disabled={disabled}
        spellCheck={false}
        style={{
          ...sharedStyles,
          position: "relative",
          width: "100%",
          height: "100%",
          minHeight: "inherit",
          resize: "none",
          background: "transparent",
          color: "transparent",
          caretColor: "#d4d4d4",
          WebkitTextFillColor: "transparent",
          zIndex: 1,
        }}
      />
    </div>
  );
}
