# frozen_string_literal: true

require 'spec_helper'
require 'securerandom'

# Quack funnel mode against a real server
#
# The suite starts this server itself. See spec/support/quack_server.rb for the server code.
#
# Only a real server can decide these things:
# - Whether session state persists across statements
# - What a funneled write returns
# - How the client reacts to the server's catalog
#
# quack_spec.rb covers the funnel's generated SQL without a server.
#
# These tests exercise both server kinds. The difference between the two kinds matters for several
# examples:
# - A plain DuckDB file behind Quack stops accepting new connections once a table exists with a
#   computed column default.
# - DuckLake never produces such a default.
# - DuckLake also does not support PRIMARY KEY or RETURNING.
RSpec.describe 'Quack funnel mode', :quack do
  # These tables declare their own id column, instead of using id:. The adapter's integer primary key
  # carries a nextval default, and this default is itself under test here. DuckLake also rejects
  # PRIMARY KEY. Declaring the id column keeps these examples inside the funnel and away from both
  # problems.
  def create_table_with_explicit_id(connection, name, &block)
    connection.create_table(name, id: false, force: true) do |t|
      t.string :id
      block&.call(t)
    end
  end

  def model_for(table)
    Class.new(ActiveRecord::Base) do
      self.table_name = table
      self.primary_key = 'id'
    end.tap(&:reset_column_information)
  end

  QuackServer::KINDS.each do |kind|
    describe "serving #{kind}" do
      let(:server) { QuackServer.for(kind) }
      let(:connection) { ActiveRecord::Base.connection }

      before { ActiveRecord::Base.establish_connection(server.config) }
      after { ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected? }

      it 'funnels statements into the server session rather than the local database' do
        expect(connection).to be_quack
        expect(connection.select_value('SELECT current_database()')).to eq(QuackServer::DATABASE)
      end

      it 'detects the served database kind through the funnel' do
        expect(connection.ducklake?).to be(kind == :ducklake)
      end

      it 'creates a table on the server and sees it through introspection' do
        create_table_with_explicit_id(connection, :qk_tables) { |t| t.string :name }

        expect(connection.tables).to include('qk_tables')
        expect(connection.table_exists?('qk_tables')).to be(true)
        expect(connection.table_exists?('qk_absent')).to be(false)
      end

      it 'reads and writes through the funnel' do
        create_table_with_explicit_id(connection, :qk_crud) { |t| t.string :name }
        model = model_for('qk_crud')
        2.times { |i| model.create!(id: SecureRandom.uuid, name: "row#{i}") }

        expect(model.count).to eq(2)
        expect(model.where(name: 'row0').update_all(name: 'renamed')).to eq(1)
        expect(model.order(:name).pluck(:name)).to eq(%w[renamed row1])
        expect(model.where(name: 'row1').delete_all).to eq(1)
        expect(model.count).to eq(1)
      end

      # Funnel mode always turns prepared_statements off. So #quote is the only path values take.
      # This is why this test checks sub-second precision and binary payloads here specifically.
      it 'round-trips values that bind parameters would normally have carried' do
        create_table_with_explicit_id(connection, :qk_values) do |t|
          t.string   :name
          t.datetime :occurred_at
          t.binary   :payload
        end
        moment = Time.utc(2026, 8, 26, 12, 30, 45, 123_456)
        payload = "\x00\xFF\x01abc".b

        model_for('qk_values').create!(
          id: SecureRandom.uuid, name: "it's quoted", occurred_at: moment, payload: payload
        )
        row = model_for('qk_values').first

        expect(row.name).to eq("it's quoted")
        expect(row.occurred_at.usec).to eq(123_456)
        expect(row.payload.b).to eq(payload)
      end

      it 'holds a transaction open across statements and rolls it back' do
        create_table_with_explicit_id(connection, :qk_tx) { |t| t.string :name }
        model = model_for('qk_tx')

        connection.transaction do
          model.create!(id: SecureRandom.uuid, name: 'doomed')
          expect(model.count).to eq(1)
          raise ActiveRecord::Rollback
        end

        expect(model.count).to eq(0)
      end

      it 'refuses to funnel a statement carrying bind parameters' do
        bind = ActiveRecord::Relation::QueryAttribute.new('x', 1, ActiveRecord::Type::Integer.new)

        expect { connection.exec_query('SELECT ?', 'BINDS', [bind]) }
          .to raise_error(ActiveRecord::ConnectionAdapters::QuackBindParametersNotSupported)
      end
    end
  end

  describe 'serving plain DuckDB' do
    let(:server) { QuackServer.for(:plain) }
    let(:connection) { ActiveRecord::Base.connection }

    before { ActiveRecord::Base.establish_connection(server.config) }
    after { ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected? }

    it 'reports the id of a funneled INSERT ... RETURNING' do
      create_table_with_explicit_id(connection, :qk_returning) { |t| t.string :name }
      given = SecureRandom.uuid

      result = connection.exec_query(
        %{INSERT INTO "qk_returning" ("id", "name") VALUES ('#{given}', 'a') RETURNING "id"}, 'RET'
      )

      expect(result.columns).to eq(['id'])
      expect(result.rows.flatten.first).to eq(given)
    end
  end

  # This is the regression that makes plain DuckDB unusable behind Quack. The ATTACH step replicates
  # the server's catalog. It also binds every column default along the way. So one computed default
  # locks out every later connection. The connection that created the table keeps working, though.
  # Proving the lockout needs a second connection for that reason.
  describe 'a second connection to a plain server' do
    let(:server) { QuackServer.for(:plain) }

    def new_connection
      ActiveRecord::ConnectionAdapters::DuckdbAdapter.new(server.config).tap(&:connect!)
    end

    it 'connects when no table has a computed column default' do
      first = new_connection
      create_table_with_explicit_id(first, :qk_literal_default) do |t|
        t.string  :name
        t.boolean :active, default: false # a literal default binds fine
      end

      second = new_connection
      expect(second.select_value('SELECT 1')).to eq(1)

      [first, second].each(&:disconnect!)
    end

    it 'fails naming the offending column once a computed default exists' do
      first = new_connection
      # The adapter's own integer primary key carries the computed default in question
      first.create_table(:qk_int_pk, force: true) { |t| t.string :name }

      expect { new_connection }.to raise_error(
        ActiveRecord::ConnectionAdapters::QuackAttachmentFailed, /qk_int_pk\.id DEFAULT nextval/
      )

      first.disconnect!
    ensure
      # Undo the change on the server. Otherwise every later example against this server gets locked
      # out too. Only the server itself is still reachable at this point.
      server.run_on_server('DROP TABLE IF EXISTS qk_int_pk')
    end
  end
end
