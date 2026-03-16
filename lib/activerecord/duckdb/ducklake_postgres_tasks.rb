# frozen_string_literal: true

module Activerecord
  module Duckdb
    # Standalone tasks to create and drop a PostgreSQL database used as a DuckLake backend.
    # Uses only the +pg+ gem. Caller passes connection
    # params explicitly (e.g. from +secrets.postgres+ in their DuckLake config).
    class DucklakePostgresTasks
      class << self
        # Creates the PostgreSQL database. No-op if the +pg+ gem is not available.
        # @param connection_params [Hash] Postgres connection params (symbol or string keys):
        #   +database+ (required), +host+, +port+, +user+ or +username+, +password+
        # @return [void]
        # @raise [ArgumentError] if +database+ is missing
        def create(connection_params)
          with_maintenance_connection(connection_params, 'create') do |conn, db_name|
            conn.exec("CREATE DATABASE #{quote_ident(db_name)}")
          end
        end

        # Drops the PostgreSQL database. Terminates existing connections first. No-op if +pg+ is not available.
        # @param connection_params [Hash] same as for +create+
        # @return [void]
        # @raise [ArgumentError] if +database+ is missing
        def drop(connection_params)
          with_maintenance_connection(connection_params, 'drop') do |conn, db_name|
            conn.exec_params(
              'SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()',
              [db_name.to_s]
            )
            conn.exec("DROP DATABASE IF EXISTS #{quote_ident(db_name)}")
          end
        end

        # Extracts +secrets.postgres+ from a database config hash for use with +create+ / +drop+.
        # @param db_config [ActiveRecord::DatabaseConfigurations::DatabaseConfig, Hash] config or config hash
        # @return [Hash, nil] connection params with symbol keys, or nil if +secrets.postgres+ is missing
        def connection_params_from_db_config(db_config)
          hash = db_config.is_a?(Hash) ? db_config : db_config.configuration_hash
          hash.with_indifferent_access.dig(:secrets, :postgres)
        end

        private

        def with_maintenance_connection(connection_params, action)
          return unless pg_gem_available?

          params = normalize_params(connection_params)
          db_name = params[:database]
          raise ArgumentError, 'connection_params must include :database' if db_name.nil? || db_name.to_s.empty?

          conn = connect_maintenance(params)
          yield conn, db_name
          conn.close
        rescue StandardError => e
          conn&.close
          raise StandardError, "Couldn't #{action} PostgreSQL database '#{db_name}': #{e.message}"
        end

        def pg_gem_available?
          return @pg_gem_available if defined?(@pg_gem_available)

          @pg_gem_available = begin
            require 'pg'
            true
          rescue LoadError
            false
          end
        end

        def normalize_params(connection_params)
          params = connection_params.transform_keys(&:to_sym)
          params[:user] ||= params[:username]
          params
        end

        def connect_maintenance(params)
          opts = {
            dbname: 'postgres',
            host: params[:host],
            port: params[:port],
            user: params[:user],
            password: params[:password]
          }.compact
          require 'pg'
          PG.connect(opts)
        end

        def quote_ident(name)
          return name if name.to_s.empty?

          "\"#{name.to_s.gsub('"', '""')}\""
        end
      end
    end
  end
end
