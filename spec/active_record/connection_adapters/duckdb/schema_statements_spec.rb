# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::SchemaStatements do
  let(:connection) { ActiveRecord::Base.connection }

  before do
    ActiveRecord::Base.establish_connection(adapter: 'duckdb', database: ':memory:')

    connection.create_table(:authors) { |t| t.string :name, index: true }
    connection.create_table(:tags, id: :integer) { |t| t.string :label }
  end

  after do
    ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
  end

  # Rails 8.2's schema dumper asks these readers about all tables at once.
  describe 'schema readers given an Array of tables' do
    it 'answers primary_keys per table' do
      expect(connection.primary_keys(%w[authors tags])).to eq('authors' => ['id'], 'tags' => ['id'])
    end

    it 'answers indexes per table' do
      indexes = connection.indexes(%i[authors tags])

      expect(indexes.keys).to eq(%w[authors tags])
      expect(indexes['authors'].map(&:columns)).to eq([['name']])
      expect(indexes['tags']).to eq([])
    end

    it 'answers table_options per table' do
      expect(connection.table_options(%w[authors tags]))
        .to eq('authors' => connection.table_options('authors'), 'tags' => connection.table_options('tags'))
    end

    it 'still answers for a single table' do
      expect(connection.primary_keys('authors')).to eq(['id'])
    end
  end

  describe '#create_table' do
    it 'puts the sequence default on the primary key in the CREATE TABLE statement' do
      statements = []
      subscriber = ActiveSupport::Notifications.subscribe('sql.active_record') do |*, payload|
        statements << payload[:sql]
      end

      connection.create_table(:posts) { |t| t.string :title }

      expect(statements.grep(/CREATE TABLE "posts"/).first)
        .to include(%{"id" BIGINT DEFAULT nextval('posts_id_seq') PRIMARY KEY})
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    it 'fills the primary key from the sequence' do
      connection.execute("INSERT INTO authors (name) VALUES ('Ada'), ('Grace')")

      expect(connection.select_values('SELECT id FROM authors ORDER BY id')).to eq([1, 2])
    end
  end
end
