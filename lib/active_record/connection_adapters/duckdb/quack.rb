# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    # Raised when a statement with bind parameters must be funneled to a Quack server.
    # A Quack server accepts SQL as text only.
    class QuackBindParametersNotSupported < NotImplementedError
      def initialize(sql = nil)
        super('Bind parameters cannot be sent to a Quack server, the statement is passed on as ' \
              "text. Inline the values instead.#{" Statement: #{sql}" if sql}")
      end
    end

    # Raised when the ATTACH to a Quack server fails.
    # This error gives the diagnosis for one cause that is not obvious: a computed column
    # default anywhere on the server.
    class QuackAttachmentFailed < ActiveRecord::ActiveRecordError
      # @param uri [String] The server that could not be attached
      # @param cause_message [String] First line of the underlying DuckDB error
      # @param suspects [Array<Array>] Rows of [database, table, column, default] that may be the cause
      def initialize(uri, cause_message, suspects = [])
        super(<<~MESSAGE.chomp)
          Could not attach the Quack server at #{uri}: #{cause_message}

          A "Catalog does not exist" error here means a table the server serves has a computed
          column default. Attaching binds every column default in the server's catalog: a literal
          binds fine, one needing a function or an operator does not, and the ATTACH then fails for
          every new connection while the one that created the table keeps working. The catalog is
          not scoped to one database, so the table may be in one this connection never uses.
          #{suspect_report(suspects)}
          Drop the default on the server (ALTER TABLE <t> ALTER COLUMN <c> DROP DEFAULT) or avoid
          creating one: this adapter emits DEFAULT nextval(...) for integer primary keys outside
          DuckLake mode, while `id: :uuid` and literal defaults such as `default: false` are safe.
        MESSAGE
      end

      private

      # @param suspects [Array<Array>] Rows of [database, table, column, default]
      # @return [String] A report padded with blank lines, or an empty string when there is nothing to report
      def suspect_report(suspects)
        return '' if suspects.empty?

        listed = suspects.first(10).map { |db, table, column, default| "  #{db}.#{table}.#{column} DEFAULT #{default}" }
        more = suspects.size > 10 ? ["  ... and #{suspects.size - 10} more"] : []
        (["\nComputed defaults found on the server:"] + listed + more + ['']).join("\n")
      end
    end

    module Duckdb
      # Sends every statement to a DuckDB server through the Quack client/server protocol.
      #
      # Quack exposes the server as an attached database. That attachment covers only a subset of
      # SQL:
      #
      #   - No UPDATE or DELETE ("Can only update base table")
      #   - No ALTER
      #   - No metadata: information_schema comes back empty
      #   - No statement that scans more than one remote table. This rules out joins, subqueries,
      #     and INSERT ... SELECT
      #
      # This code does not sort statements by what the attachment supports. Instead, it sends
      # every statement to the server as-is. The server then behaves like a local DuckDB, at no
      # measurable cost. Funneled queries match direct queries in throughput and return the same
      # types. Nothing resolves against the local catalog. So the client needs no USE statement
      # and no cache invalidation.
      #
      # How a statement gets there depends on the DuckDB version the client runs:
      #
      #   - DuckDB 2.0 and later: the session runs +CONNECT quack+ once, as the last step of
      #     #configure_connection. From then on, DuckDB itself forwards every statement on that
      #     connection to the server, including SET, so nothing may run locally after it.
      #   - DuckDB 1.5: +CONNECT+ does not exist yet. This code wraps every statement in
      #
      #       SELECT * FROM quack.query('<statement>')
      #
      #     DuckDB 2.0 names +CONNECT+ as the successor of this wrapper.
      #
      # Both paths reach the same server-side call, so both behave the same.
      #
      # Configure Quack with a +quack+ section on the database:
      #
      #   ducklake:
      #     adapter: duckdb
      #     database: :memory
      #     extensions:
      #       - quack
      #     quack:
      #       uri: quack:localhost
      #       token: <%= ENV['DUCKLAKE_QUACK_TOKEN'] %>
      #       database: ducklake
      #       disable_ssl: false # true when the server is addressed by anything but localhost
      #
      # No table on the server may have a *computed* column default. The ATTACH binds every
      # column default in the server's catalog. A default that needs a function or an operator
      # makes the ATTACH fail for good. This adapter gives every integer primary key a
      # +DEFAULT nextval('<table>_id_seq')+. So the first create_table call outside DuckLake mode
      # locks out every later connection. See QuackAttachmentFailed, which names the column at
      # fault.
      module Quack
        # The name under which the server is attached. Only the funnel itself uses this name.
        ATTACHMENT = 'quack'

        # The first DuckDB release with the CONNECT statement
        CONNECT_VERSION = Gem::Version.new('2.0')

        # Whether a DuckDB library routes statements with CONNECT.
        #
        # This check drops the pre-release part, so a 2.0 alpha counts as 2.0. The alpha builds
        # already ship CONNECT.
        #
        # @param version [String] A DuckDB library version, such as "1.5.5" or "2.0.0-alpha39998"
        # @return [Boolean]
        def self.connect_supported?(version = DuckDB::LIBRARY_VERSION)
          Gem::Version.new(version.to_s.delete_prefix('v')).release >= CONNECT_VERSION
        rescue ArgumentError
          false
        end

        # The +quack+ section of the database config
        # @return [Hash, nil] config with symbol keys, or nil when no Quack server is configured
        def quack_config
          return @quack_config if defined?(@quack_config)

          @quack_config = @config[:quack]&.symbolize_keys
        end

        # Whether statements are funneled to a Quack server
        # @return [Boolean]
        def quack?
          !quack_config.nil?
        end

        # Whether statements reach the server through CONNECT (DuckDB 2.0+), not the query() wrapper
        # @return [Boolean]
        def quack_connect?
          quack? && Quack.connect_supported?
        end

        # Whether this connection has already run CONNECT.
        #
        # From that point on, every statement runs on the server. This includes probes that are
        # meant for the local database, such as the one in #configuration_locked?. The check is
        # tied to the raw connection object, so a reconnect starts over.
        #
        # @return [Boolean]
        def quack_connected?
          !raw_connection.nil? && @quack_connected_on.equal?(raw_connection)
        end

        # Whether ActiveRecord may keep bind parameters separate from the statement text.
        #
        # It may not. A funnel carries SQL as text only and has no place for bind parameters.
        # This method turns prepared statements off, so ActiveRecord inlines the values into the
        # statement itself. The funnel needs this: internal queries, such as the lookup for
        # ar_internal_metadata, use bind parameters otherwise.
        # #quack_sql still raises an error if any bind parameters arrive anyway.
        #
        # @return [Boolean]
        def prepared_statements?
          return false if quack?

          super
        end
        # The superclass aliases the reader method to the predicate method. This override must
        # point the alias at the new method too
        alias prepared_statements prepared_statements?

        # Prepares a statement to run in the server session. Returns the statement unchanged when
        # no Quack server is set up. Callers may call this method for every statement, with no
        # check first.
        #
        # With CONNECT, DuckDB forwards the statement itself, so this method returns it unchanged.
        # Without CONNECT, this method wraps it in the query() table function.
        #
        # @param sql [String] The statement to run
        # @param binds [Array] Bind parameters of the statement. Neither path can carry them
        # @return [String] The statement to hand to DuckDB
        # @raise [QuackBindParametersNotSupported] if the statement has bind parameters
        def quack_sql(sql, binds = [])
          return sql unless quack?
          # CONNECT refuses parameterized statements as well. This error names the cause, not
          # DuckDB's own
          raise QuackBindParametersNotSupported, sql if binds.any?
          return sql if quack_connect?

          "SELECT * FROM #{ATTACHMENT}.query(#{quote(sql)})"
        end

        # Attaches the configured Quack server. Called during #configure_connection.
        # @return [void]
        def attach_quack
          return unless quack?

          raw_connection.execute(attach_quack_sql)
        rescue DuckDB::Error => e
          raise QuackAttachmentFailed.new(quack_config[:uri], e.message.lines.first.to_s.strip,
                                          computed_default_suspects)
        end

        # Points this session at the attached server. Called last in #configure_connection.
        #
        # From DuckDB 2.0 on, this method runs CONNECT. CONNECT sends every later statement on
        # this connection to the server, including SET. So nothing that must run locally may
        # come after it, lock_configuration included.
        #
        # @return [void]
        def enter_quack
          return unless quack?

          if quack_connect?
            raw_connection.execute("CONNECT #{ATTACHMENT}")
            @quack_connected_on = raw_connection
          end

          # The attachment exposes whichever database is current in the server session. A USE
          # statement in the server's own startup script does not carry over to clients. So each
          # connection must switch the session itself. After this, unqualified names resolve in
          # that database.
          database = quack_config[:database]
          raw_connection.execute(quack_sql("USE #{quote_database_name(database)}")) if database.present?
        end

        # Quotes a database name for a USE statement, one part at a time. This keeps the
        # separator in a qualified +catalog.schema+ name.
        #
        # @param database [String] The database name from the config
        # @return [String] The quoted name
        def quote_database_name(database)
          database.to_s.split('.').map { |part| quote_column_name(part) }.join('.')
        end

        # The ATTACH that connects this client to the Quack server.
        #
        # The client picks the scheme from the host name. Localhost means plain HTTP. Every
        # other host name means HTTPS. What the server speaks depends on its DuckDB version:
        #
        #   - DuckDB 1.5: only plain HTTP. So reaching a server by a service name needs
        #     +disable_ssl: true+, or needs TLS terminated by a proxy in front of the server.
        #   - DuckDB 2.0: HTTPS on any host but localhost, with a self-signed certificate unless
        #     the server is given its own. The client trusts a self-signed certificate only when
        #     +ssl_fingerprint+ pins it. SSL_FINGERPRINT implies HTTPS, even on localhost.
        #
        # @return [String] the ATTACH statement
        def attach_quack_sql
          options = []
          # This code quotes the token instead of interpolating it. A token is opaque text. An
          # apostrophe in a token would otherwise end the string literal early and cause a
          # parser error at connect time
          options << "TOKEN #{quote(quack_config[:token])}" if quack_config[:token].present?
          options << "DISABLE_SSL #{quack_disable_ssl}" unless quack_disable_ssl.nil?
          # DuckDB 1.5 rejects this option, so it goes out only when configured
          options << "SSL_FINGERPRINT #{quote(quack_config[:ssl_fingerprint])}" if quack_config[:ssl_fingerprint].present?

          sql = "ATTACH #{quote(quack_config[:uri])} AS #{ATTACHMENT}"
          sql << " (#{options.join(", ")})" unless options.empty?
          sql
        end

        # Number of rows a statement changed.
        # A funneled write reports this number in a single +Count+ column, not through the normal result.
        # @param raw_result [DuckDB::Result] The raw DuckDB result
        # @return [Integer] Number of rows affected
        def affected_rows(raw_result)
          return super unless quack? && quack_count_result?(raw_result)

          raw_result.to_a.first&.first.to_i
        end

        private

        # Column defaults on the server that may explain why the ATTACH failed.
        #
        # This method asks the question through quack_query. quack_query takes the server URI
        # instead of a catalog, so it still gives an answer when ATTACH cannot. This method runs
        # the query on the raw connection. The raw connection does not go through #log. So the
        # token is inlined here, and it must never reach the query log. This is best effort by
        # design. A failed probe must never hide the error that it was meant to explain.
        #
        # @return [Array<Array>] Rows of [database, table, column, default]
        def computed_default_suspects
          sql = <<~SQL.squish
            SELECT database_name, table_name, column_name, column_default
            FROM duckdb_columns() WHERE column_default IS NOT NULL
          SQL
          rows = raw_connection.query(quack_query_sql(sql)).to_a
          rows.select { |row| computed_default?(row[3]) }
        rescue StandardError
          []
        end

        # Whether a stored column default needs a function or an operator to bind.
        #
        # DuckDB stores a boolean literal as CAST('f' AS BOOLEAN). This form binds fine. So a
        # leading cast alone is not suspicious. This method is a hint for an error message. It
        # is not a full parser.
        #
        # @param default [String, nil] The stored default expression
        # @return [Boolean]
        def computed_default?(default)
          text = default.to_s.strip
          return false if text.empty?
          return true if text.match?(/\ACURRENT_(TIMESTAMP|DATE|TIME)\z/i)

          text.include?('(') && !text.match?(/\ACAST\s*\(/i)
        end

        # A statement wrapped for quack_query. quack_query needs no attachment.
        # @param sql [String] The statement to run on the server
        # @return [String] The wrapping statement
        def quack_query_sql(sql)
          args = [quote(quack_config[:uri]), quote(sql)]
          args << "token := #{quote(quack_config[:token])}" if quack_config[:token].present?
          args << "disable_ssl := #{quack_disable_ssl}" unless quack_disable_ssl.nil?
          args << "ssl_fingerprint := #{quote(quack_config[:ssl_fingerprint])}" if quack_config[:ssl_fingerprint].present?

          "SELECT * FROM quack_query(#{args.join(", ")})"
        end

        # The configured +disable_ssl+ as SQL, or nil when it is unset.
        #
        # This method relies on Rails' own type coercion. YAML and ENV variables pass this value
        # as the text 'false', not as a real boolean.
        #
        # @return [String, nil] 'true', 'false' or nil
        def quack_disable_ssl
          disable_ssl = quack_config[:disable_ssl]
          return if disable_ssl.nil?

          self.class.type_cast_config_to_boolean(disable_ssl) ? 'true' : 'false'
        end

        # Whether a result is the row count that Quack reports back for a funneled write
        # @param raw_result [DuckDB::Result] The raw DuckDB result
        # @return [Boolean]
        def quack_count_result?(raw_result)
          return false unless raw_result.respond_to?(:columns)

          column = raw_result.columns.first
          column && (column.respond_to?(:name) ? column.name : column.to_s) == 'Count'
        end
      end
    end
  end
end
