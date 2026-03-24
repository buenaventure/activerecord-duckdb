# frozen_string_literal: true

require 'active_record/tasks/database_tasks'
require 'activerecord/duckdb/ducklake_postgres_tasks'
require 'duckdb'

module ActiveRecord
  module Tasks # :nodoc:
    # Database tasks implementation for DuckDB adapter
    class DuckdbDatabaseTasks # :nodoc:
      # Keys that indicate in-memory database mode
      MEMORY_MODE_KEYS = [:memory, 'memory', ':memory:', ':memory'].freeze
      TRUE_VALUES = [true, 'true', 'TRUE', 1, '1'].freeze

      # Indicates whether this adapter uses database configurations
      # @return [Boolean] always returns true
      def self.using_database_configurations?
        true
      end

      # Initializes a new DuckDB database tasks instance
      # @param db_config [ActiveRecord::DatabaseConfigurations::DatabaseConfig] Database configuration
      # @param root_path [String] Root path for the Rails application
      def initialize(db_config, root_path = ActiveRecord::Tasks::DatabaseTasks.root)
        @db_config = db_config
        @configuration_hash = db_config.configuration_hash.with_indifferent_access
        @root_path = root_path
      end

      # Creates a new DuckDB database file or skips if in-memory mode.
      # When using DuckLake with a Postgres backend, also creates the PostgreSQL database (if the pg gem is available).
      # @return [void]
      # @raise [StandardError] if database creation fails
      def create
        run_ducklake_postgres_task(:create)

        # For DuckDB, creating a database means creating the file (if not in-memory)
        database_path = @configuration_hash[:database]
        return if MEMORY_MODE_KEYS.include?(database_path)

        # Ensure the directory exists
        FileUtils.mkdir_p(File.dirname(database_path)) if database_path.include?('/')

        # Create the database file by opening a connection
        DuckDB::Database.open(database_path).connect.close
      rescue StandardError => e
        warn "Couldn't create '#{database_path}' database. Please check your configuration."
        warn "Error: #{e.message}"
        raise
      end

      # Drops the DuckDB database by removing the database file.
      # When using DuckLake with a local DATA_PATH, also removes that data directory (rm_rf).
      # When using DuckLake with a Postgres backend, also drops the PostgreSQL database (if the pg gem is available).
      # @return [void]
      # @raise [StandardError] if database drop fails
      def drop
        db_path = @configuration_hash[:database]
        unless MEMORY_MODE_KEYS.include?(db_path)
          db_file_path = File.absolute_path?(db_path) ? db_path : File.join(root_path, db_path)
          FileUtils.rm_f(db_file_path)
        end

        ducklake_data_paths.each do |path|
          FileUtils.rm_rf(path) if File.directory?(path)
        end

        run_ducklake_postgres_task(:drop)
      rescue StandardError => e
        warn "Couldn't drop database '#{db_path}'"
        warn "Error: #{e.message}"
        raise
      end

      # Purges the database by dropping and recreating it
      # @return [void]
      def purge
        drop
        create
      end

      # Returns the character set used by DuckDB
      # @return [String] always returns 'UTF-8'
      def charset
        'UTF-8'
      end

      # Returns the collation used by DuckDB
      # @return [nil] always returns nil as DuckDB doesn't use explicit collations
      def collation
        nil
      end

      # Ensures destructive tasks are not run against a protected environment (Rails 8.1+).
      # @param db_config [ActiveRecord::DatabaseConfigurations::DatabaseConfig]
      # @param migration_class [Class] typically ActiveRecord::Base
      # @return [void]
      # @raise [ActiveRecord::ProtectedEnvironmentError, ActiveRecord::EnvironmentMismatchError]
      def check_current_protected_environment!(db_config, migration_class)
        with_temporary_pool_for_migration_check(db_config, migration_class) do |pool|
          migration_context = pool.migration_context
          current = migration_context.current_environment
          stored = migration_context.last_stored_environment

          if migration_context.protected_environment?
            raise ActiveRecord::ProtectedEnvironmentError.new(stored)
          end

          if stored && stored != current
            raise ActiveRecord::EnvironmentMismatchError.new(current: current, stored: stored)
          end
        rescue ActiveRecord::NoDatabaseError
        end
      end

      # Dumps the database structure to a SQL file
      # @param filename [String] The filename to write the structure dump to
      # @param extra_flags [String, nil] Additional flags for the dump (unused)
      # @return [void]
      def structure_dump(filename, extra_flags = nil)
        # Export the database schema
        establish_connection

        File.open(filename, 'w') do |file|
          # Get all tables using DuckDB's information schema
          tables_sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
          tables = connection.query(tables_sql)

          tables.each do |table_row|
            table_name = table_row[0]
            next if %w[ar_internal_metadata schema_migrations].include?(table_name)

            # Get table schema using PRAGMA (DuckDB supports SQLite-compatible PRAGMA)
            table_info = connection.query("PRAGMA table_info('#{table_name}')")

            # Build CREATE TABLE statement
            # [[0, "id", "BIGINT", true, nil, true], [1, "name", "VARCHAR", false, 'default_value', false], [2, "email", "VARCHAR", false, nil, false], [3, "created_at", "TIMESTAMP", true, nil, false], [4, "updated_at", "TIMESTAMP", true, nil, false]]
            columns = table_info.map do |col|
              col_def = "#{col[1]} #{col[2]}"
              col_def += ' NOT NULL' if TRUE_VALUES.include?(col[3])
              col_def += " DEFAULT #{col[4]}" if col[4]
              col_def += ' PRIMARY KEY' if TRUE_VALUES.include?(col[5])
              col_def
            end

            file.puts "CREATE TABLE #{table_name} ("
            file.puts "  #{columns.join(",\n  ")}"
            file.puts ');'
            file.puts
          end

          # Get list of sequences used for the table
          sequences = connection.query('SELECT sequencename, start_value, min_value, max_value, increment_by, cycle FROM pg_catalog.pg_sequences').to_a

          # Build CREATE SEQUENCE statements
          sequences.each do |seq|
            seq_def = "CREATE SEQUENCE #{seq[0]}"
            seq_def += " START WITH #{seq[1]}" if seq[1] && seq[1] != 1
            seq_def += " INCREMENT BY #{seq[4]}" if seq[4] && seq[4] != 1
            seq_def += " MINVALUE #{seq[2]}" if seq[2] != -9_223_372_036_854_775_808
            seq_def += " MAXVALUE #{seq[3]}" if seq[3] != 9_223_372_036_854_775_807
            seq_def += ' CYCLE' if seq[5]
            seq_def += ';'
            file.puts seq_def
          end
        end
      end

      # Loads database structure from a SQL file
      # @param filename [String] The filename to load the structure from
      # @param extra_flags [String, nil] Additional flags for the load (unused)
      # @return [void]
      # @raise [LoadError] if the schema file does not exist
      def structure_load(filename, extra_flags = nil)
        establish_connection
        raise(LoadError, 'Database scheme file does not exist') unless File.exist?(filename)

        sql = File.read(filename)
        connection.query(sql)
      end

      private

      attr_reader :configuration_hash, :db_config, :root_path

      def with_temporary_pool_for_migration_check(db_config, migration_class, clobber: false)
        original_db_config = migration_class.connection_db_config
        pool = migration_class.connection_handler.establish_connection(db_config, clobber: clobber)

        yield pool
      ensure
        migration_class.connection_handler.establish_connection(original_db_config, clobber: clobber)
      end

      def run_ducklake_postgres_task(action)
        return unless ducklake_using_postgres?

        params = Activerecord::Duckdb::DucklakePostgresTasks.connection_params_from_db_config(@db_config)
        Activerecord::Duckdb::DucklakePostgresTasks.public_send(action, params)
      rescue StandardError => e
        db_name = params&.dig(:database)
        warn "Couldn't #{action} PostgreSQL database '#{db_name}' for DuckLake. Please check secrets and connectivity."
        warn "Error: #{e.message}"
        raise
      end

      # @return [Array<String>] absolute paths to local DuckLake DATA_PATH directories from attachments
      def ducklake_data_paths
        attachments = configuration_hash[:attachments] || []
        paths = []
        attachments.each do |a|
          conn = a[:connection_string].to_s
          next unless conn.start_with?('ducklake')

          options = a[:options].to_s
          next if options.empty?

          # DATA_PATH '...' or DATA_PATH "..."
          options.scan(/DATA_PATH\s+['"]([^'"]+)['"]/i).each do |(path)|
            path = path.strip
            next if path.match?(%r{\A(s3|https?|gs)://}i) # skip remote

            path = File.join(root_path, path) unless File.absolute_path?(path)
            paths << path
          end
        end
        paths.uniq
      end

      # @return [Boolean] true if config uses DuckLake with a Postgres backend
      def ducklake_using_postgres?
        attachments = configuration_hash[:attachments] || []
        has_ducklake_postgres = attachments.any? do |a|
          conn = (a[:connection_string] || a['connection_string']).to_s
          conn.start_with?('ducklake:postgres')
        end
        return false unless has_ducklake_postgres

        params = Activerecord::Duckdb::DucklakePostgresTasks.connection_params_from_db_config(@db_config)
        params && params[:database].present?
      end

      # Gets a database connection from the connection pool
      # @return [ActiveRecord::ConnectionAdapters::AbstractAdapter] The database connection
      def connection
        if ActiveRecord::Base.connection_pool
          ActiveRecord::Base.connection_pool.checkout
        else
          ActiveRecord::Base.lease_connection
        end
      end

      # Establishes a connection to the database
      # @param config [ActiveRecord::DatabaseConfigurations::DatabaseConfig] Database configuration to use
      # @return [void]
      def establish_connection(config = db_config)
        ActiveRecord::Base.establish_connection(config)
      end
    end
  end
end
