## [Unreleased]

### Added

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

## [0.1.0] - 2025-06-18

- Initial release
