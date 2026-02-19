curl -sS -X POST http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "tool": "load_csv",
    "args": {
      "path": "C:\Users\pchitnbh\Documents\Workday Data Table.csv",
      "delimiter": ",",
      "encoding": null,
      "sample_rows": 10
    }
  }'