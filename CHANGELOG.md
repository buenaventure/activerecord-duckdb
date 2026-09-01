## [Unreleased]

### Added

- Add Quack funnel mode. A `quack:` section on the database config points at a DuckDB server that
  serves DuckLake over the Quack client/server protocol. The adapter funnels every statement to it.
  See the README.
- Add `QuackAttachmentFailed`. This error is raised when the `ATTACH` to a Quack server fails. No
  table the server serves may have a computed column default. Attaching binds every default in the
  server's catalog, so one `nextval()`, `uuid()`, or `now()` stops every later connection from
  attaching. To find the cause, the error probes the server through `quack_query`, which needs no
  attachment, and names the offending `database.table.column`.
- Add `Quoting#quoted_binary`. This lets the adapter write binary columns without bind parameters.

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
