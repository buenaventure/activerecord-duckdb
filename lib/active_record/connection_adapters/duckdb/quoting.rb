# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module Quoting
        extend ActiveSupport::Concern

        module ClassMethods
          # Quotes a column name for use in SQL statements
          # @param name [String, Symbol] The column name to quote
          # @return [String] The quoted column name wrapped in double quotes
          def quote_column_name(name)
            %("#{name}")
          end
        end

        # Quotes a table name for use in SQL statements
        # @param name [String, Symbol] The table name to quote
        # @return [String] The quoted table name (delegates to quote_column_name)

        def quote_table_name(name)
          quote_column_name(name)
        end

        # Quotes a column name for use in SQL statements
        # @param name [String, Symbol] The column name to quote
        # @return [String] The quoted column name wrapped in double quotes
        def quote_column_name(name)
          %("#{name}")
        end

        # Quotes a value for safe inclusion in SQL statements
        #
        # Every value passes through this method when bind parameters are off, for example
        # with +prepared_statements: false+. Every value also passes through this method in
        # Quack funnel mode. This method must handle the same data as bind parameters,
        # including binary payloads and sub-second timestamps.
        #
        # @param value [Object] The value to quote
        # @return [String] The appropriately quoted value for SQL
        def quote(value)
          case value
          when String
            "'#{value.gsub("'", "''")}'"
          when nil
            'NULL'
          when true
            'TRUE'
          when false
            'FALSE'
          when Numeric
            value.to_s
          when ActiveRecord::Type::Binary::Data
            quoted_binary(value)
          when Time, DateTime
            # #quoted_date keeps sub-second precision. It also follows ActiveRecord.default_timezone
            "'#{quoted_date(value)}'"
          when Date
            "'#{value.strftime("%Y-%m-%d")}'"
          else
            "'#{value.to_s.gsub("'", "''")}'"
          end
        end

        # Quotes a binary payload as a DuckDB BLOB literal.
        #
        # This method uses hex encoding, not escaping. A BLOB can hold any byte value. A NUL
        # byte or a backslash inside a quoted string literal causes a parser error or silent
        # corruption.
        #
        # @param value [Object] The binary value, typically an ActiveRecord::Type::Binary::Data
        # @return [String] A BLOB literal
        def quoted_binary(value)
          "unhex('#{value.to_s.unpack1("H*")}')::BLOB"
        end
      end
    end
  end
end
