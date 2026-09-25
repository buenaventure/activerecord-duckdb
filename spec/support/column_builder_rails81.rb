# frozen_string_literal: true

# Rails 8.1+ column builder
# Column constructor: (name, cast_type, default, sql_type_metadata, null, default_function, ...)
module ColumnBuilderRails81
  # Rails 8.1's Column calls cast_type.mutable? and cast_type.deserialize(default). A plain
  # Value answers false and returns the default unchanged.
  #
  # This must be a real type, not an rspec double. Column.new deduplicates columns through a
  # registry that outlives the example, and it compares a new column with a registered one
  # whenever their hashes collide. A double from an earlier example then raises "has leaked into
  # another example", depending on garbage collection timing.
  def build_column(name, default, metadata, null: true, **)
    ActiveRecord::ConnectionAdapters::Duckdb::Column.new(name, ActiveModel::Type::Value.new, default, metadata, null, **)
  end
end
