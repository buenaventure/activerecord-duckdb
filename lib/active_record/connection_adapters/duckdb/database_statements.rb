# frozen_string_literal: true

# This module provides database statements for the DuckDB adapter.
module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module DatabaseStatements
        # SQL statements that are considered read-only (SELECT, EXPLAIN, etc.)
        READ_QUERY_PATTERN = /\A\s*(SELECT|SHOW|DESCRIBE|EXPLAIN|PRAGMA)\b/i

        # Begins a database transaction.
        # @return [void]
        def begin_db_transaction
          log('BEGIN', 'TRANSACTION') do
            with_raw_connection do |conn|
              # Transaction control must run where the writes run. A BEGIN sent to the local
              # client does not cover writes that reach a Quack server. A BEGIN sent through
              # the funnel covers all writes.
              conn.query(quack_sql('BEGIN TRANSACTION'))
            end
          end
        end

        # Commits the current database transaction.
        # @return [void]
        def commit_db_transaction
          log('COMMIT', 'TRANSACTION') do
            with_raw_connection do |conn|
              conn.query(quack_sql('COMMIT'))
            end
          end
        end

        # Rolls back the current database transaction.
        # @return [void]
        def exec_rollback_db_transaction
          log('ROLLBACK', 'TRANSACTION') do
            with_raw_connection do |conn|
              conn.query(quack_sql('ROLLBACK'))
            end
          end
        end

        # Determines if a SQL query is a write operation (INSERT, UPDATE, DELETE, etc.)
        # Rails' own readonly guard asks this before it runs a statement, so a read passes on a
        # connection that prevents writes. Also used for transaction tracking.
        # @param sql [String] The SQL query to check
        # @return [Boolean] true if the query modifies data
        def write_query?(sql)
          !READ_QUERY_PATTERN.match?(sql)
        end

        # Casts a DuckDB result to ActiveRecord::Result format.
        # Rails calls this with the result of #perform_query (see Duckdb::Compat).
        # @param result [DuckDB::Result, nil] The DuckDB result to cast
        # @return [ActiveRecord::Result] The ActiveRecord-compatible result
        def cast_result(result)
          return ActiveRecord::Result.empty if result.nil?

          columns = result.columns.map do |col|
            if col.respond_to?(:name)
              col.name
            elsif col.respond_to?(:column_name)
              col.column_name
            else
              col.to_s
            end
          end

          ActiveRecord::Result.new(columns, result.to_a)
        end

        # Returns the number of affected rows from a raw DuckDB result.
        # Rails calls this with the result of #perform_query for DELETE and UPDATE statements.
        # @param raw_result [DuckDB::Result] The raw DuckDB result
        # @return [Integer] Number of rows affected
        def affected_rows(raw_result)
          raw_result.rows_changed
        end

        # Returns columns that should be included in INSERT statements
        # @param table_name [String] The name of the table
        # @return [Array<ActiveRecord::ConnectionAdapters::Column>] Columns to include in INSERT
        def columns_for_insert(table_name)
          columns(table_name).reject do |column|
            # Exclude columns that have a default function (like nextval)
            column.default_function.present?
          end
        end

        # Extracts the last inserted ID from an insert result
        # @param result [ActiveRecord::Result] The result from an insert operation
        # @return [Object] The last inserted ID value
        def last_inserted_id(result)
          # Handle ActiveRecord::Result from RETURNING clause
          if result.is_a?(ActiveRecord::Result) && result.rows.any?
            id_value = result.rows.first.first
            return id_value
          end
          super
        end

        private

        # Runs a statement on the raw DuckDB connection. Every Rails version reaches this through
        # its own #perform_query hook (see Duckdb::Compat), after logging, retries, the readonly
        # guard and the query transformers are set up.
        # @param raw_connection [DuckDB::Connection] The connection to run the statement on
        # @param sql [String] The statement
        # @param type_casted_binds [Array] Bind values as DuckDB receives them
        # @return [DuckDB::Result] The raw DuckDB result
        def duckdb_query(raw_connection, sql, type_casted_binds)
          sql = quack_sql(sql, type_casted_binds)

          if type_casted_binds.empty?
            raw_connection.query(sql)
          else
            raw_connection.query(sql, *type_casted_binds)
          end
        end
      end
    end
  end
end
