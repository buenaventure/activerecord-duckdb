# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::Quack do
  # An unconnected adapter is enough for these tests. The funnel is a pure string transformation. It
  # needs no server to build.
  def adapter(config = {})
    ActiveRecord::ConnectionAdapters::DuckdbAdapter.new({ adapter: 'duckdb', database: ':memory:' }.merge(config))
  end

  let(:quack_config) { { quack: { uri: 'quack:localhost', token: 'secret', database: 'ducklake' } } }

  describe 'without a quack section in the config' do
    it 'is not in funnel mode' do
      expect(adapter).not_to be_quack
    end

    it 'passes statements through unchanged' do
      expect(adapter.quack_sql('SELECT 1')).to eq('SELECT 1')
    end

    it 'accepts bind parameters' do
      expect(adapter.quack_sql('SELECT ?', [1])).to eq('SELECT ?')
    end

    it 'leaves prepared statements alone' do
      expect(adapter.prepared_statements?).to be(true)
    end

    it 'runs statements against the local database' do
      with_memory_connection do |conn|
        expect(conn).not_to be_quack
        expect(query_value('SELECT 42', connection: conn)).to eq(42)
      end
    end
  end

  describe 'with a quack section in the config' do
    subject(:conn) { adapter(quack_config) }

    it 'is in funnel mode' do
      expect(conn).to be_quack
    end

    it 'exposes the config with symbol keys' do
      expect(conn.quack_config).to eq(uri: 'quack:localhost', token: 'secret', database: 'ducklake')
    end

    it 'wraps a statement into a query on the attachment' do
      expect(conn.quack_sql('SELECT 1')).to eq("SELECT * FROM quack.query('SELECT 1')")
    end

    it 'escapes quotes in the wrapped statement' do
      expect(conn.quack_sql("SELECT 'it''s'")).to eq("SELECT * FROM quack.query('SELECT ''it''''s''')")
    end

    it 'wraps statements of every kind, not just queries' do
      expect(conn.quack_sql('ALTER TABLE t ADD COLUMN c INTEGER'))
        .to eq("SELECT * FROM quack.query('ALTER TABLE t ADD COLUMN c INTEGER')")
    end

    describe 'the ATTACH it builds' do
      it 'passes the token' do
        expect(conn.attach_quack_sql).to eq("ATTACH 'quack:localhost' AS quack (TOKEN 'secret')")
      end

      it 'omits DISABLE_SSL when unconfigured, leaving the client to pick by hostname' do
        expect(conn.attach_quack_sql).not_to include('DISABLE_SSL')
      end

      it 'forces plain HTTP when disable_ssl is set' do
        sql = adapter(quack: quack_config[:quack].merge(disable_ssl: true)).attach_quack_sql
        expect(sql).to eq("ATTACH 'quack:localhost' AS quack (TOKEN 'secret', DISABLE_SSL true)")
      end

      it "treats the string 'false' as false, the way YAML and ENV deliver it" do
        sql = adapter(quack: quack_config[:quack].merge(disable_ssl: 'false')).attach_quack_sql
        expect(sql).to include('DISABLE_SSL false')
      end

      it 'works without a token' do
        sql = adapter(quack: { uri: 'quack:localhost', disable_ssl: true }).attach_quack_sql
        expect(sql).to eq("ATTACH 'quack:localhost' AS quack (DISABLE_SSL true)")
      end

      it 'quotes a token that contains an apostrophe' do
        sql = adapter(quack: quack_config[:quack].merge(token: "secr'et")).attach_quack_sql
        expect(sql).to eq("ATTACH 'quack:localhost' AS quack (TOKEN 'secr''et')")
      end

      it 'quotes the uri, so it cannot end the literal early' do
        sql = adapter(quack: quack_config[:quack].merge(uri: "quack:local'host")).attach_quack_sql
        expect(sql).to eq("ATTACH 'quack:local''host' AS quack (TOKEN 'secret')")
      end

      it 'keeps a token from injecting further ATTACH options' do
        sql = adapter(quack: quack_config[:quack].merge(token: "x'), DISABLE_SSL true) --")).attach_quack_sql
        expect(sql).to eq(%{ATTACH 'quack:localhost' AS quack (TOKEN 'x''), DISABLE_SSL true) --')})
      end
    end

    describe 'the database name it switches to' do
      it 'quotes the name' do
        expect(conn.quote_database_name('ducklake')).to eq('"ducklake"')
      end

      it 'quotes each part of a qualified name, keeping the separator' do
        expect(conn.quote_database_name('lake.main')).to eq('"lake"."main"')
      end
    end

    it 'turns prepared statements off, so ActiveRecord inlines values instead of binding them' do
      expect(conn.prepared_statements?).to be(false)
      expect(conn.prepared_statements).to be(false)
    end

    it 'refuses bind parameters instead of dropping them' do
      expect { conn.quack_sql('SELECT ?', [1]) }
        .to raise_error(ActiveRecord::ConnectionAdapters::QuackBindParametersNotSupported,
                        /Inline the values instead/)
    end
  end
end
