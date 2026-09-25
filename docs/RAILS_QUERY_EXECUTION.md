# Rails Version Compatibility

This document explains how the DuckDB adapter supports several Rails versions (8.0, 8.1, and the unreleased 8.2 on Rails `main`) from a single codebase.

## Approach

Almost all of the adapter does not depend on the Rails version: quoting, schema introspection, the schema dumper, DuckLake and the Quack funnel. Rails' own internal APIs do change between minor versions, though. Where one changed, a small module under `lib/active_record/connection_adapters/duckdb/compat/` fills the gap, and `Duckdb::Compat` picks one module per gap.

`Duckdb::Compat` is the only place that reads `ActiveRecord::VERSION`:

```ruby
# lib/active_record/connection_adapters/duckdb/compat.rb
RAILS_VERSION = Gem::Version.new(ActiveRecord::VERSION::STRING).release  # 8.2.0.alpha counts as 8.2

def self.included(adapter)
  adapter.include(rails?('>= 8.1') ? ColumnRails81 : ColumnRails80)
  adapter.include(rails?('>= 8.2') ? QueryRails82 : QueryRails80)
end
```

| Rails | Query execution | Column constructor |
|-------|-----------------|--------------------|
| 8.0 | `Compat::QueryRails80` | `Compat::ColumnRails80` |
| 8.1 | `Compat::QueryRails80` | `Compat::ColumnRails81` |
| 8.2 | `Compat::QueryRails82` | `Compat::ColumnRails81` |

Rails 7.2 is not supported. It reached end of life in August 2026.

## Query Execution

The adapter hands every statement to Rails' query pipeline and only implements the lowest hook, `perform_query`. Rails does the logging, retries, warning handling, query transformers, and the readonly guard. The actual DuckDB call lives in one shared private method, `DatabaseStatements#duckdb_query`. It wraps the statement for the Quack funnel (`quack_sql`) and runs it on the raw connection.

### Rails 8.0 / 8.1

```
execute / exec_query / select_all / insert / ...
  └── internal_execute(sql, ...)            # readonly guard (write_query?), query transformers
        └── raw_execute(sql, ...)           # log, with_raw_connection, retries, warnings
              └── perform_query(raw_connection, sql, binds, type_casted_binds, prepare:, notification_payload:, batch:)
                    └── duckdb_query(raw_connection, sql, type_casted_binds)     # Compat::QueryRails80
```

### Rails 8.2

Rails 8.2 removed `raw_execute`, `internal_execute`, and `internal_exec_query`. Every statement is a `QueryIntent`:

```
execute / exec_query / select_all / query_command / execute_batch / ...
  └── QueryIntent#execute!
        └── #processed_sql                  # readonly guard (write_query?), query transformers
        └── execute_intent(intent)          # instrumentation, retries, warnings
              └── perform_query(raw_connection, intent)
                    └── duckdb_query(raw_connection, intent.processed_sql, intent.type_casted_binds)  # Compat::QueryRails82
                    └── verified!
```

Rails 8.2 no longer marks the connection as verified after each query. `QueryRails82#perform_query` calls `verified!` itself, as the adapters that ship with Rails do.

### Transactions

`BEGIN`, `COMMIT`, and `ROLLBACK` go through the same pipeline. `transaction_command` uses `internal_execute` on 8.0/8.1 and `query_command` on 8.2, because Rails 8.2 deprecates `log`. So transaction control reaches a Quack server through the funnel, just as the writes do.

### Readonly Guard

Rails asks the adapter's `write_query?` before it runs a statement on a connection that prevents writes. `write_query?` uses Rails' own read query pattern (`AbstractAdapter.build_read_query_regexp(:show, :describe, :pragma)`). That pattern treats `SELECT`, `WITH`, `EXPLAIN`, transaction control, `SHOW`, `DESCRIBE`, and `PRAGMA` as reads, and it skips leading comments.

## Column Constructor (Rails 8.1)

Rails 8.1 added the cast type to the `Column` constructor:

```ruby
# Rails 8.0
def initialize(name, default, sql_type_metadata, null, default_function, ...)

# Rails 8.1+
def initialize(name, cast_type, default, sql_type_metadata, null, default_function, ...)
```

`Compat::ColumnRails80` and `Compat::ColumnRails81` each implement `new_column_from_field` with the matching signature. Both read the field through the shared `column_info_from_field`.

## Other Rails 8.2 Changes

These needed no compat module, because the new behaviour works on every supported version:

- **Batched schema readers.** The Rails 8.2 schema dumper asks `primary_keys`, `indexes`, and `table_options` about all tables at once, passing an Array of table names. Given an Array, these readers answer with a Hash keyed by table name. `columns` falls back to `column_definitions` per table in Rails itself.
- **`create_table` runs through `execute_batch`.** The primary key's `DEFAULT nextval(...)` is therefore emitted by `SchemaCreation` when it builds the column, not by rewriting the finished `CREATE TABLE` statement.
- **Deprecations.** `exec_insert`, `exec_delete`, and `exec_update` are deprecated in 8.2. The adapter does not call them.

## Adding a New Rails Version

1. Add an appraisal (for an unreleased version, point it at `github: 'rails/rails', branch: 'main'`) and run `bundle exec appraisal install`.
2. Add the version to the `rails` matrix in `.github/workflows/tests.yml`.
3. Run the suite against it. For each Rails API that changed, add a module under `compat/` and pick it in `Compat.included`. If the new behaviour also works on the older versions, change the shared code instead.

### When to Reconsider Separate Branches

The single codebase pays off while the compat modules stay small next to the shared code. Today they are four modules of a few lines each. If a future Rails version needs compat code comparable in size to the shared code, maintaining a branch and gem version per Rails version becomes the cheaper option.

## Testing with Appraisal

```bash
# All versions
bundle exec appraisal rspec

# Specific version
bundle exec appraisal rails-8.0 rspec
bundle exec appraisal rails-8.1 rspec
bundle exec appraisal rails-main rspec
```

## File Structure

```
lib/active_record/connection_adapters/duckdb/
├── compat.rb                       # Picks the modules for the loaded Rails version
├── compat/
│   ├── column_rails80.rb           # Rails 8.0: new_column_from_field
│   ├── column_rails81.rb           # Rails 8.1+: new_column_from_field (with cast_type)
│   ├── query_rails80.rb            # Rails 8.0/8.1: perform_query(raw_connection, sql, ...)
│   └── query_rails82.rb            # Rails 8.2+: perform_query(raw_connection, intent)
├── database_statements.rb          # Shared: duckdb_query, cast_result, affected_rows, write_query?
├── schema_creation.rb              # Shared: primary key sequence default
└── schema_statements.rb            # Shared schema operations
```
