## [Unreleased]

### Removed

- Remove Rails 7.2 support. Rails 7.2 reached end of life in August 2026. The gem now requires
  ActiveRecord `>= 8.0, < 8.3`.

### Added

- Add support for Rails 8.2, tested against Rails `main` until 8.2 is released. Rails 8.2
  replaces `raw_execute` with the `QueryIntent` pipeline, and its schema dumper asks
  `primary_keys`, `indexes`, and `table_options` about many tables at once. Given an Array of
  tables, these readers now answer with a Hash keyed by table name.
- Add Quack funnel mode. A `quack:` section on the database config points at a DuckDB server that
  serves DuckLake over the Quack client/server protocol. The adapter funnels every statement to it.
  See the README.
- Add `QuackAttachmentFailed`. This error is raised when the `ATTACH` to a Quack server fails. No
  table the server serves may have a computed column default. Attaching binds every default in the
  server's catalog, so one `nextval()`, `uuid()`, or `now()` stops every later connection from
  attaching. To find the cause, the error probes the server through `quack_query`, which needs no
  attachment, and names the offending `database.table.column`.
- Add `Quoting#quoted_binary`. This lets the adapter write binary columns without bind parameters.

### Changed

- Run every statement through Rails' own query pipeline. The adapter now implements only
  `perform_query`. Rails supplies the logging, retries, query transformers, and the readonly
  guard. This includes `BEGIN`, `COMMIT`, and `ROLLBACK`. The query log now shows the statement
  the application issued, not the Quack funnel wrapper around it.
- Put the version-specific code in one place, `Duckdb::Compat`. See
  `docs/RAILS_QUERY_EXECUTION.md`.
- `write_query?` uses Rails' read query pattern. A read behind a leading SQL comment, a `WITH`
  query, and transaction control now pass on a connection that prevents writes.
- Emit the primary key's `DEFAULT nextval(...)` when the `CREATE TABLE` statement is built. The
  adapter no longer rewrites the finished statement in `#execute`.

### Fixed

- Fix quoted timestamps. They now keep their sub-second precision. They now honour
  `ActiveRecord.default_timezone`. This fix applies with `prepared_statements: false`, and always
  in Quack mode.
- Fix quoted binary values. They no longer fail with `Parser Error: unterminated quoted string`.
- Fix an empty column or table name. The adapter now refuses it with a message that names the
  likely cause. Before this fix, the name reached DuckDB as `Parser Error: zero-length delimited
  identifier`. `#update`, `#destroy`, and `#reload` produced this error on a DuckLake record.
- Fix schema introspection. It is now scoped to the current database. `information_schema.tables`
  and `duckdb_indexes()` span every attached database. So, before this fix, `tables`,
  `table_exists?`, and `indexes` could answer for the wrong attachment.
- Fix DuckLake schema introspection. It now ignores historic table versions (`end_snapshot IS
  NULL`). This includes the partitioning in `ducklake_partition_info`, which is versioned
  separately. So repartitioning a table no longer dumps both the old and the new partition columns.
- Fix `ducklake_table_options`. It no longer drops a table's options from the schema dump when the
  table carries a metadata key outside the dumpable set.
- Fix `sequence_exists?`. It now reads `duckdb_sequences()` instead of consuming a value with
  `nextval()` on every call. DuckDB does not roll a sequence back. So, before this fix, a table
  recreated with `force: true` started at id 2.
- Fix `sequences`. It now returns the current database's sequences, instead of always `[]`.
- Fix `create_table` with an explicit `id:` type (`:uuid`, `:string`, `:bigint`, `:integer`) in
  DuckLake mode. Before this fix, only the default `id` type omitted the `PRIMARY KEY` constraint
  that DuckLake rejects. DuckLake tables also no longer attempt to create the sequence that DuckLake
  cannot create.

## [0.1.0] - 2025-06-18

- Initial release
