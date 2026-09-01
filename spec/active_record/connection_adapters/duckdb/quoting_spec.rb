# frozen_string_literal: true

require 'spec_helper'

RSpec.describe ActiveRecord::ConnectionAdapters::Duckdb::Quoting do
  let(:config) do
    {
      adapter: 'duckdb',
      database: ':memory:'
    }
  end

  let(:adapter) { ActiveRecord::ConnectionAdapters::DuckdbAdapter.new(nil, nil, {}, config) }

  before do
    adapter.send(:connect)
  end

  after do
    adapter.disconnect
  end

  describe '#quote' do
    describe 'string values' do
      it 'wraps simple strings in single quotes' do
        expect(adapter.quote('hello')).to eq("'hello'")
        expect(adapter.quote('world')).to eq("'world'")
      end

      it 'escapes single quotes by doubling them' do
        expect(adapter.quote("it's")).to eq("'it''s'")
        expect(adapter.quote("don't")).to eq("'don''t'")
      end

      it 'handles multiple consecutive single quotes' do
        expect(adapter.quote("'''")).to eq("''''''''")
        expect(adapter.quote("can't won't")).to eq("'can''t won''t'")
      end

      it 'handles empty strings' do
        expect(adapter.quote('')).to eq("''")
      end

      it 'preserves special characters as-is' do
        result = adapter.quote('hello\nworld')
        expect(result).to start_with("'")
        expect(result).to end_with("'")
        expect(result).to include('hello')
        expect(result).to include('world')
      end

      it 'handles unicode strings' do
        expect(adapter.quote('héllo')).to eq("'héllo'")
        expect(adapter.quote('世界')).to eq("'世界'")
        expect(adapter.quote('🌟')).to eq("'🌟'")
      end
    end

    describe 'nil values' do
      it 'returns NULL for nil' do
        expect(adapter.quote(nil)).to eq('NULL')
      end
    end

    describe 'boolean values' do
      it 'returns TRUE for true' do
        expect(adapter.quote(true)).to eq('TRUE')
      end

      it 'returns FALSE for false' do
        expect(adapter.quote(false)).to eq('FALSE')
      end
    end

    describe 'numeric values' do
      it 'converts integers to strings without quotes' do
        expect(adapter.quote(42)).to eq('42')
        expect(adapter.quote(0)).to eq('0')
        expect(adapter.quote(-123)).to eq('-123')
      end

      it 'converts floats to strings without quotes' do
        expect(adapter.quote(3.14)).to eq('3.14')
        expect(adapter.quote(-2.5)).to eq('-2.5')
        expect(adapter.quote(0.0)).to eq('0.0')
      end

      it 'handles large numbers' do
        expect(adapter.quote(9_999_999_999_999_999)).to eq('9999999999999999')
      end

      it 'handles BigDecimal values' do
        big_decimal = BigDecimal('123.456')
        expect(adapter.quote(big_decimal)).to eq('123.456')
      end

      it 'handles Rational values' do
        rational = Rational(1, 3)
        expect(adapter.quote(rational)).to eq(rational.to_s)
      end
    end

    describe 'other object types' do
      it 'converts objects to strings and quotes them' do
        expect(adapter.quote(:symbol)).to eq("'symbol'")
      end

      it 'handles arrays by converting to string' do
        result = adapter.quote([1, 2, 3])
        expect(result).to start_with("'")
        expect(result).to end_with("'")
        expect(result).to include('1')
        expect(result).to include('2')
        expect(result).to include('3')
      end

      it 'handles hashes by converting to string' do
        hash = { key: 'value' }
        result = adapter.quote(hash)
        expect(result).to start_with("'")
        expect(result).to end_with("'")
        expect(result).to include('key')
        expect(result).to include('value')
      end

      it 'handles custom objects via to_s' do
        custom_object = Object.new
        allow(custom_object).to receive(:to_s).and_return('custom_string')
        expect(adapter.quote(custom_object)).to eq("'custom_string'")
      end
    end

    describe 'date and time objects' do
      it 'converts Date objects to quoted strings' do
        date = Date.new(2023, 12, 25)
        expect(adapter.quote(date)).to eq("'2023-12-25'")
      end

      it 'converts Time objects to quoted strings' do
        time = Time.new(2023, 12, 25, 14, 30, 0)
        result = adapter.quote(time)
        expect(result).to start_with("'")
        expect(result).to end_with("'")
        expect(result).to include('2023-12-25')
      end

      it 'converts DateTime objects to quoted strings' do
        datetime = DateTime.new(2023, 12, 25, 14, 30, 0)
        result = adapter.quote(datetime)
        expect(result).to start_with("'")
        expect(result).to end_with("'")
        expect(result).to include('2023-12-25')
      end

      # Everything below matters because #quote is the only path values take once bind parameters
      # are off. Bind parameters are always off in Quack funnel mode.
      it 'keeps sub-second precision' do
        time = Time.utc(2026, 8, 26, 12, 30, 45, 123_456)
        expect(adapter.quote(time)).to eq("'2026-08-26 12:30:45.123456'")
      end

      it 'honours ActiveRecord.default_timezone rather than always converting to UTC' do
        time = Time.new(2026, 8, 26, 12, 30, 45, '+02:00')
        original = ActiveRecord.default_timezone
        begin
          ActiveRecord.default_timezone = :local
          expect(adapter.quote(time)).to eq("'#{adapter.quoted_date(time)}'")
          expect(adapter.quote(time)).to include('12:30:45')
        ensure
          ActiveRecord.default_timezone = original
        end
      end
    end

    describe 'binary values' do
      let(:payload) { "\x00\xFF\x01abc".b }
      let(:binary) { ActiveRecord::Type::Binary::Data.new(payload) }

      it 'renders a BLOB literal instead of a quoted string' do
        expect(adapter.quote(binary)).to eq("unhex('00ff01616263')::BLOB")
      end

      it 'round-trips bytes that no quoted string could carry' do
        expect(adapter.select_value("SELECT #{adapter.quote(binary)}").b).to eq(payload)
      end

      it 'handles an empty payload' do
        expect(adapter.quote(ActiveRecord::Type::Binary::Data.new(''))).to eq("unhex('')::BLOB")
      end
    end
  end

  describe '#quote_column_name' do
    it 'wraps column names in double quotes' do
      expect(adapter.quote_column_name('id')).to eq('"id"')
      expect(adapter.quote_column_name('name')).to eq('"name"')
      expect(adapter.quote_column_name('email')).to eq('"email"')
    end

    it 'handles column names with special characters' do
      expect(adapter.quote_column_name('user_id')).to eq('"user_id"')
      expect(adapter.quote_column_name('first-name')).to eq('"first-name"')
      expect(adapter.quote_column_name('column with spaces')).to eq('"column with spaces"')
    end

    it 'protects reserved words' do
      expect(adapter.quote_column_name('select')).to eq('"select"')
      expect(adapter.quote_column_name('from')).to eq('"from"')
      expect(adapter.quote_column_name('where')).to eq('"where"')
      expect(adapter.quote_column_name('order')).to eq('"order"')
    end

    it 'handles edge cases' do
      expect(adapter.quote_column_name('123')).to eq('"123"')
      expect(adapter.quote_column_name('1column')).to eq('"1column"')
    end

    # An empty identifier is never valid SQL. In practice, it comes from a DuckLake model with no
    # primary key. On #update, #destroy and #reload, ActiveRecord quotes the nil key into
    # `WHERE "" = ...`. DuckDB answers with a parser error that names neither the table nor the
    # cause. The adapter refuses the empty identifier here instead, so it can give a clearer message.
    it 'refuses an empty column name rather than emitting a zero-length identifier' do
      expect { adapter.quote_column_name('') }
        .to raise_error(ActiveRecord::ActiveRecordError, /empty column or table name.*primary key/m)
      expect { adapter.quote_column_name(nil) }
        .to raise_error(ActiveRecord::ActiveRecordError, /empty column or table name.*primary key/m)
      expect { adapter.quote_table_name(nil) }
        .to raise_error(ActiveRecord::ActiveRecordError, /empty column or table name.*primary key/m)
      expect { adapter.class.quote_column_name(nil) }
        .to raise_error(ActiveRecord::ActiveRecordError, /empty column or table name.*primary key/m)
    end

    it 'handles unicode column names' do
      expect(adapter.quote_column_name('名前')).to eq('"名前"')
      expect(adapter.quote_column_name('año')).to eq('"año"')
    end

    it 'converts symbols to strings' do
      expect(adapter.quote_column_name(:id)).to eq('"id"')
      expect(adapter.quote_column_name(:user_name)).to eq('"user_name"')
    end
  end

  describe '#quote_table_name' do
    it 'delegates to quote_column_name' do
      expect(adapter.quote_table_name('users')).to eq('"users"')
      expect(adapter.quote_table_name('posts')).to eq('"posts"')
      expect(adapter.quote_table_name('user_profiles')).to eq('"user_profiles"')
    end

    it 'handles table names with special characters' do
      expect(adapter.quote_table_name('table-name')).to eq('"table-name"')
      expect(adapter.quote_table_name('table with spaces')).to eq('"table with spaces"')
    end

    it 'protects reserved words' do
      expect(adapter.quote_table_name('user')).to eq('"user"')
      expect(adapter.quote_table_name('group')).to eq('"group"')
      expect(adapter.quote_table_name('order')).to eq('"order"')
    end

    it 'handles schema-qualified table names' do
      expect(adapter.quote_table_name('schema.table')).to eq('"schema.table"')
    end

    it 'handles symbols' do
      expect(adapter.quote_table_name(:users)).to eq('"users"')
      expect(adapter.quote_table_name(:user_posts)).to eq('"user_posts"')
    end
  end

  describe 'SQL injection prevention' do
    it 'safely escapes malicious string content' do
      malicious_string = "'; DROP TABLE users; --"
      quoted = adapter.quote(malicious_string)
      expect(quoted).to eq("'''; DROP TABLE users; --'")
      # Content is safely contained within quotes
      expect(quoted).to start_with("'")
      expect(quoted).to end_with("'")
    end

    it 'safely contains malicious column names' do
      malicious_column = 'id"; DROP TABLE users; --'
      quoted = adapter.quote_column_name(malicious_column)
      expect(quoted).to eq('"id"; DROP TABLE users; --"')
      expect(quoted).to start_with('"')
      expect(quoted).to end_with('"')
    end

    it 'escapes complex injection attempts' do
      malicious_value = "' OR '1'='1"
      quoted = adapter.quote(malicious_value)
      expect(quoted).to eq("''' OR ''1''=''1'")
    end
  end

  describe 'edge cases' do
    it 'handles very long strings efficiently' do
      long_string = 'a' * 10_000
      quoted = adapter.quote(long_string)
      expect(quoted).to start_with("'")
      expect(quoted).to end_with("'")
      expect(quoted.length).to eq(long_string.length + 2)
    end

    it 'handles strings containing only quotes' do
      expect(adapter.quote("'")).to eq("''''")
      expect(adapter.quote("''")).to eq("''''''")
    end

    it 'handles binary data' do
      binary_data = "\x00\x01\x02\xFF".dup.force_encoding('BINARY')
      quoted = adapter.quote(binary_data)
      expect(quoted).to start_with("'")
      expect(quoted).to end_with("'")
    end

    it 'distinguishes between empty string and nil' do
      expect(adapter.quote('')).to eq("''")
      expect(adapter.quote(nil)).to eq('NULL')
    end
  end

  describe 'ActiveRecord integration' do
    it 'works with ActiveRecord type casting' do
      string_attr = ActiveRecord::Type::String.new.cast('test')
      expect(adapter.quote(string_attr)).to eq("'test'")

      integer_attr = ActiveRecord::Type::Integer.new.cast('42')
      expect(adapter.quote(integer_attr)).to eq('42')

      boolean_attr = ActiveRecord::Type::Boolean.new.cast('true')
      expect(adapter.quote(boolean_attr)).to eq('TRUE')

      boolean_attr_false = ActiveRecord::Type::Boolean.new.cast('false')
      expect(adapter.quote(boolean_attr_false)).to eq('FALSE')
    end

    it 'handles ActiveRecord decimal values' do
      decimal_attr = ActiveRecord::Type::Decimal.new.cast('123.45')
      expect(adapter.quote(decimal_attr)).to eq('123.45')
    end

    it 'handles ActiveRecord date and time values' do
      date_attr = ActiveRecord::Type::Date.new.cast('2023-12-25')
      expect(adapter.quote(date_attr)).to eq("'2023-12-25'")

      datetime_attr = ActiveRecord::Type::DateTime.new.cast('2023-12-25 14:30:00')
      result = adapter.quote(datetime_attr)
      expect(result).to start_with("'")
      expect(result).to end_with("'")
      expect(result).to include('2023-12-25')
    end
  end

  describe 'Rails conventions compliance' do
    it 'uses double quotes for identifiers' do
      expect(adapter.quote_column_name('test')).to start_with('"')
      expect(adapter.quote_column_name('test')).to end_with('"')
    end

    it 'uses single quotes for string literals' do
      expect(adapter.quote('test')).to start_with("'")
      expect(adapter.quote('test')).to end_with("'")
    end

    it 'uses standard boolean literals' do
      expect(adapter.quote(true)).to eq('TRUE')
      expect(adapter.quote(false)).to eq('FALSE')
    end

    it 'uses NULL for nil values' do
      expect(adapter.quote(nil)).to eq('NULL')
    end
  end

  describe 'performance characteristics' do
    it 'handles strings with many quotes efficiently' do
      string_with_many_quotes = "'" * 1000
      start_time = Time.current
      quoted = adapter.quote(string_with_many_quotes)
      end_time = Time.current

      expect(quoted).to eq("'#{"''" * 1000}'")
      expect(end_time - start_time).to be < 0.1
    end
  end
end
