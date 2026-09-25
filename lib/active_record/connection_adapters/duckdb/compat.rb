# frozen_string_literal: true

require 'active_record/connection_adapters/duckdb/compat/column_rails80'
require 'active_record/connection_adapters/duckdb/compat/column_rails81'
require 'active_record/connection_adapters/duckdb/compat/query_rails80'
require 'active_record/connection_adapters/duckdb/compat/query_rails82'

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      # The one place that knows which Rails version is loaded.
      #
      # Most of the adapter does not depend on the Rails version. Where Rails changed an internal
      # API, a small module under compat/ fills the gap, and this module picks one per gap when it
      # is included into the adapter class.
      #
      # To support a new Rails version, add a module under compat/ for each API that changed and
      # pick it below.
      module Compat
        # The loaded ActiveRecord version, without any prerelease part, so that 8.2.0.alpha
        # counts as 8.2.
        RAILS_VERSION = Gem::Version.new(ActiveRecord::VERSION::STRING).release

        # Whether the loaded ActiveRecord version satisfies a requirement.
        # @param requirement [String] A gem requirement, such as '>= 8.1'
        # @return [Boolean]
        def self.rails?(requirement)
          Gem::Requirement.new(requirement).satisfied_by?(RAILS_VERSION)
        end

        # Includes the modules that match the loaded Rails version.
        # @param adapter [Class] The adapter class
        # @return [void]
        def self.included(adapter)
          super

          # Rails 8.1 passes the cast type to the Column constructor.
          adapter.include(rails?('>= 8.1') ? ColumnRails81 : ColumnRails80)
          # Rails 8.2 replaced raw_execute with the QueryIntent pipeline.
          adapter.include(rails?('>= 8.2') ? QueryRails82 : QueryRails80)
        end
      end
    end
  end
end
