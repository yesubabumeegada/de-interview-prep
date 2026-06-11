export const ICON_COLORS: Record<string, string> = {
  aws: "#FF9900",
  databricks: "#FF3621",
  snowflake: "#29B5E8",
  hadoop: "#66CCFF",
  oracle: "#F80000",
  teradata: "#F37440",
  python: "#3776AB",
  pyspark: "#E25A1C",
  sql: "#4169E1",
  airflow: "#017CEE",
  bash: "#4EAA25",
  kafka: "#231F20",
  nifi: "#728E9B",
  dbt: "#FF694A",
  "ai-rag": "#D97757",
  ai: "#412991",
  gcp: "#4285F4",
  cicd: "#2088FF",
  git: "#181717",
  kubernetes: "#2496ED",
  lakehouse: "#00ADD4",
  powerbi: "#F2C811",
  // stroke icons
  etl: "#6366f1",
  "data-modeling": "#8b5cf6",
  "data-quality": "#10b981",
  streaming: "#ef4444",
  governance: "#f59e0b",
  "system-design": "#3b82f6",
  interview: "#06b6d4",
};

export function getIconColor(iconKey: string): string {
  return ICON_COLORS[iconKey] ?? "#6366f1";
}

export function getIconBg(iconKey: string, alpha = 0.12): string {
  const hex = getIconColor(iconKey);
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${alpha})`;
}
