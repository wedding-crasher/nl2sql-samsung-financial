### Expected Dataset Direcotry Structure

```
../dataset/
├── NL2SQL-Bugs-with-evidence.json ← ✅ Bug 및 evidence 포함 JSON
├── bird_benchmark/
│   ├── dev.json ← Sample file
│   ├── dev_tables.json ← (Optional) Table schema info
│   ├── dev_databases/ ← Folder containing SQLite DBs
│   │   ├── academic/
│   │   │   └── academic.sqlite
│   │   ├── college/
│   │   │   └── college.sqlite
```

> NL2SQL-BUGS Dataset: https://nl2sql-bugs.github.io/
> Official Bird Dataset: https://bird-bench.github.io/
