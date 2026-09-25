# Query Execution Call Graph

This document maps the query execution methods in the DuckDB adapter and how they plug into Rails' query pipeline. For why the adapter is split this way across Rails versions, see [RAILS_QUERY_EXECUTION.md](RAILS_QUERY_EXECUTION.md).

## Overview

The adapter implements only the lowest hook of Rails' query pipeline, `perform_query`. Rails supplies everything above it: logging, retries, warnings, query transformers, and the readonly guard. The signature of `perform_query` changed in Rails 8.2, so one small module per signature adapts it to the shared `duckdb_query`.

## Architecture

```mermaid
flowchart TB
    subgraph "Rails 8.0 / 8.1"
        public80["execute / exec_query / select_all / exec_insert / exec_delete"]
        internal_execute["internal_execute()<br/>readonly guard, query transformers"]
        raw_execute["raw_execute()<br/>log, with_raw_connection, retries"]
        public80 --> internal_execute --> raw_execute
    end

    subgraph "Rails 8.2"
        public82["execute / exec_query / select_all / query_command / execute_batch"]
        intent["QueryIntent#execute!<br/>readonly guard, query transformers"]
        execute_intent["execute_intent()<br/>instrumentation, retries"]
        public82 --> intent --> execute_intent
    end

    subgraph "Compat"
        perform80["QueryRails80#perform_query(raw_connection, sql, binds, type_casted_binds, ...)"]
        perform82["QueryRails82#perform_query(raw_connection, intent)"]
    end

    subgraph "Shared: DatabaseStatements"
        duckdb_query["duckdb_query(raw_connection, sql, type_casted_binds)"]
        quack_sql["quack_sql(sql, binds)"]
        cast_result["cast_result(raw_result)"]
        affected_rows["affected_rows(raw_result)"]
        write_query["write_query?(sql)"]
    end

    raw_execute --> perform80 --> duckdb_query
    execute_intent --> perform82 --> duckdb_query
    duckdb_query --> quack_sql
    internal_execute -.-> write_query
    intent -.-> write_query
    raw_execute -. result .-> cast_result
    raw_execute -. result .-> affected_rows
    execute_intent -. result .-> cast_result
    execute_intent -. result .-> affected_rows
```

## Query Flow Examples

### SELECT Query

```mermaid
sequenceDiagram
    participant App as Application
    participant Base as Rails pipeline
    participant Compat as Compat::QueryRails8x
    participant Adapter as DatabaseStatements
    participant DB as DuckDB

    App->>Base: User.find(1)
    Base->>Adapter: write_query?(sql) (only when writes are prevented)
    Base->>Compat: perform_query(...)
    Compat->>Adapter: duckdb_query(raw_connection, sql, binds)
    Adapter->>Adapter: quack_sql(sql, binds)
    Adapter->>DB: raw_connection.query(sql, *binds)
    DB-->>Base: DuckDB::Result
    Base->>Adapter: cast_result(raw_result)
    Adapter-->>Base: ActiveRecord::Result
    Base-->>App: User instance
```

### DELETE Query

```mermaid
sequenceDiagram
    participant App as Application
    participant Base as Rails pipeline
    participant Compat as Compat::QueryRails8x
    participant Adapter as DatabaseStatements / Quack
    participant DB as DuckDB

    App->>Base: User.delete_all
    Base->>Compat: perform_query(...)
    Compat->>Adapter: duckdb_query(raw_connection, sql, binds)
    Adapter->>DB: raw_connection.query(sql, *binds)
    DB-->>Base: DuckDB::Result
    Base->>Adapter: affected_rows(raw_result)
    Adapter-->>Base: Integer
    Base-->>App: rows deleted count
```

## Design Decisions

### 1. One Hook, Two Signatures

The adapter used to override `raw_execute`, which bypassed parts of Rails' pipeline and had to be rewritten for each Rails version. Rails 8.0 and 8.1 already call `perform_query` from `raw_execute`, and Rails 8.2 kept the name with a new signature. Implementing only `perform_query` keeps the Rails-specific code to one method per signature. It also gives every Rails version the same logging, retries, and readonly guard.

### 2. DuckDB DELETE Returns Count in Result Set

DuckDB's DELETE and UPDATE statements return the affected row count as a result set:

```sql
DELETE FROM users WHERE id = 1;
-- Returns: columns=["Count"], rows=[[1]]
```

It is also available through `result.rows_changed`, which `affected_rows` reads. A statement funneled to a Quack server reports its count only in the `Count` column. So `Quack#affected_rows` reads that column for funneled writes.

### 3. Transaction Control Uses the Pipeline

`begin_db_transaction`, `commit_db_transaction`, and `exec_rollback_db_transaction` call `transaction_command`, which runs the statement through the pipeline (`internal_execute` on 8.0/8.1, `query_command` on 8.2). The statement reaches `duckdb_query` like any other. So on a Quack connection it runs in the server session, where the writes run.

## Method Overview

| Method | Location | Purpose |
|--------|----------|---------|
| `perform_query` | `Compat::QueryRails80` / `Compat::QueryRails82` | Rails' lowest execution hook |
| `transaction_command` | `Compat::QueryRails80` / `Compat::QueryRails82` | Run BEGIN, COMMIT, or ROLLBACK through the pipeline |
| `duckdb_query` | `DatabaseStatements` (private) | Run a statement on the raw DuckDB connection |
| `cast_result` | `DatabaseStatements` | Convert a DuckDB result to `ActiveRecord::Result` |
| `affected_rows` | `DatabaseStatements`, `Quack` | Row count of a raw result |
| `write_query?` | `DatabaseStatements` | Tell reads from writes for the readonly guard |
| `quack_sql` | `Quack` | Wrap a statement for the Quack funnel |

## Schema Statements

| Method | Location | Purpose |
|--------|----------|---------|
| `tables` | Shared | List all tables |
| `indexes` | Shared | List indexes for a table, or per table for an Array |
| `primary_keys` | Shared | Primary key columns for a table, or per table for an Array |
| `table_options` | Shared | Options for the schema dump, or per table for an Array |
| `create_table` | Shared | Create a table and the sequence that fills its primary key |
| `create_sequence` | Shared | Create a DuckDB sequence |
| `type_to_sql` | Shared | Convert Rails types to DuckDB SQL |
| `new_column_from_field` | `Compat::ColumnRails80` / `Compat::ColumnRails81` | Create a Column from DB metadata |
| `fetch_type_metadata` | Shared | Parse DuckDB type strings |
