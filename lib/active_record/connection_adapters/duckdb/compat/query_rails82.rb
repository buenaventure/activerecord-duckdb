# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module Compat
        # Query execution for Rails 8.2 and later.
        #
        # Rails 8.2 removed raw_execute and internal_execute. Each statement is now a QueryIntent,
        # and the intent pipeline wraps this hook in logging, retries and warning handling. The
        # intent checks the readonly guard and runs the query transformers before it gets here.
        module QueryRails82
          # Runs the statement of a query intent on the raw DuckDB connection.
          # @param raw_connection [DuckDB::Connection] The connection to run the statement on
          # @param intent [ActiveRecord::ConnectionAdapters::QueryIntent] The statement to run
          # @return [DuckDB::Result] The raw DuckDB result
          def perform_query(raw_connection, intent)
            result = duckdb_query(raw_connection, intent.processed_sql, intent.type_casted_binds)
            # Rails 8.2 no longer marks the connection as verified after each query. The
            # adapters that ship with Rails do this themselves.
            verified!
            result
          end

          private

          # Runs a transaction control statement through the Rails query pipeline.
          # @param sql [String] BEGIN, COMMIT or ROLLBACK
          # @return [void]
          def transaction_command(sql)
            query_command(sql, 'TRANSACTION', materialize_transactions: false)
          end
        end
      end
    end
  end
end
