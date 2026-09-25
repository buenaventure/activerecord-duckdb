# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module Compat
        # Query execution for Rails 8.0 and 8.1.
        #
        # Rails' raw_execute wraps this hook in logging, retries and warning handling. Rails'
        # internal_execute checks the readonly guard and runs the query transformers before that.
        module QueryRails80
          # Runs a statement on the raw DuckDB connection.
          # @param raw_connection [DuckDB::Connection] The connection to run the statement on
          # @param sql [String] The statement, already checked and transformed by Rails
          # @param binds [Array] Bind parameters (unused, the type casted ones are sent)
          # @param type_casted_binds [Array] Bind values as DuckDB receives them
          # @param prepare [Boolean] Whether to prepare the statement (unused by DuckDB)
          # @param notification_payload [Hash] Payload of the sql.active_record notification
          # @param batch [Boolean] Whether this is a batch of statements (unused by DuckDB)
          # @return [DuckDB::Result] The raw DuckDB result
          def perform_query(raw_connection, sql, binds, type_casted_binds, prepare:, notification_payload:, batch:)
            duckdb_query(raw_connection, sql, type_casted_binds)
          end

          private

          # Runs a transaction control statement through the Rails query pipeline.
          # @param sql [String] BEGIN, COMMIT or ROLLBACK
          # @return [void]
          def transaction_command(sql)
            internal_execute(sql, 'TRANSACTION', materialize_transactions: false)
          end
        end
      end
    end
  end
end
