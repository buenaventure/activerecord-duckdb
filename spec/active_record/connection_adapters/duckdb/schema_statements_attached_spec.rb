# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::SchemaStatements do
  # DuckDB's metadata spans every attached database. So schema introspection must scope itself to the
  # current database only. Otherwise another attachment's tables look like they belong here. Rails
  # then queries tables that do not exist where it expects them.
  context 'with a second database attached' do
    let(:other_path) { "tmp/attached_#{Process.pid}.duckdb" }

    before { FileUtils.mkdir_p('tmp') }

    after { FileUtils.rm_f(other_path) }

    it 'lists only the current database when introspecting tables' do
      with_memory_connection do |conn|
        conn.execute("ATTACH '#{other_path}' AS other")
        conn.execute('CREATE TABLE other.only_over_there (id INTEGER)')
        conn.execute('CREATE TABLE right_here (id INTEGER)')

        expect(conn.tables).to include('right_here')
        expect(conn.tables).not_to include('only_over_there')
      end
    end

    it 'answers table_exists? for the current database only' do
      with_memory_connection do |conn|
        conn.execute("ATTACH '#{other_path}' AS other")
        conn.execute('CREATE TABLE other.only_over_there (id INTEGER)')

        expect(conn.table_exists?('only_over_there')).to be(false)
      end
    end
  end
end
