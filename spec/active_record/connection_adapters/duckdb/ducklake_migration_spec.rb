# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'fileutils'
require 'securerandom'

# DuckLake Migration Tests
#
# These tests check that Rails migrations create DuckLake tables.
# The tables must support all column types, including DuckDB-specific types like unsigned integers.

RSpec.describe 'DuckLake Migrations' do
  # Build a DuckLake configuration that uses local storage.
  def ducklake_config(temp_dir)
    {
      adapter: 'duckdb',
      database: ':memory:',
      extensions: ['ducklake'],
      attachments: [{
        name: 'ducklake',
        connection_string: "ducklake:#{File.join(temp_dir, 'test.ducklake')}",
        options: "DATA_PATH '#{File.join(temp_dir, 'data')}'"
      }],
      use_database: 'ducklake'
    }
  end

  describe 'creating tables with Rails migrations' do
    let(:temp_dir) { Dir.mktmpdir('ducklake_migration_test') }
    let(:connection) { ActiveRecord::Base.connection }

    before do
      FileUtils.mkdir_p(File.join(temp_dir, 'data'))
      ActiveRecord::Base.establish_connection(ducklake_config(temp_dir))
    end

    after do
      ActiveRecord::Base.remove_connection if ActiveRecord::Base.connected?
      FileUtils.rm_rf(temp_dir)
    end

    describe 'comprehensive column types table' do
      # This test creates a table with one example of each supported column type.
      before do
        connection.create_table(:all_types, id: false) do |t|
          # Standard Rails types
          t.bigint :record_id, null: false
          t.datetime :recorded_at, null: false
          t.integer :count
          t.string :label
          t.boolean :active
          t.float :ratio
          t.decimal :amount, precision: 10, scale: 2
          t.decimal :coordinates, precision: 9, scale: 6

          # Signed integer types in DuckDB
          t.tinyint :tiny_val
          t.smallint :small_val

          # Unsigned integer types in DuckDB
          t.utinyint :unsigned_tiny
          t.usmallint :unsigned_small
          t.uinteger :unsigned_int
        end
      end

      it 'creates the table successfully' do
        expect(connection.table_exists?(:all_types)).to be true
      end

      it 'creates all columns' do
        columns = connection.columns(:all_types)
        column_names = columns.map(&:name)

        expected_columns = %w[
          record_id recorded_at count label active ratio amount coordinates
          tiny_val small_val unsigned_tiny unsigned_small unsigned_int
        ]

        expected_columns.each do |col_name|
          expect(column_names).to include(col_name), "Expected column '#{col_name}' to exist"
        end
      end

      describe 'column SQL types' do
        let(:columns) { connection.columns(:all_types) }
        let(:columns_by_name) { columns.index_by(&:name) }

        # Standard Rails types mapped to DuckDB
        {
          'record_id' => 'BIGINT',
          'recorded_at' => 'TIMESTAMP',
          'count' => 'INTEGER',
          'label' => 'VARCHAR',
          'active' => 'BOOLEAN',
          'ratio' => /REAL|FLOAT/i,
          'amount' => /DECIMAL\(10,\s*2\)/i,
          'coordinates' => /DECIMAL\(9,\s*6\)/i
        }.each do |column_name, expected_type|
          it "maps #{column_name} to correct SQL type" do
            col = columns_by_name[column_name]
            expect(col).not_to be_nil, "Column '#{column_name}' not found"
            if expected_type.is_a?(Regexp)
              expect(col.sql_type).to match(expected_type)
            else
              expect(col.sql_type.upcase).to eq(expected_type.upcase)
            end
          end
        end

        # Signed integer types in DuckDB
        it 'maps tiny_val to TINYINT' do
          col = columns_by_name['tiny_val']
          expect(col.sql_type.upcase).to eq('TINYINT')
        end

        it 'maps small_val to SMALLINT' do
          col = columns_by_name['small_val']
          expect(col.sql_type.upcase).to eq('SMALLINT')
        end

        # Unsigned integer types in DuckDB
        {
          'unsigned_tiny' => 'UTINYINT',
          'unsigned_small' => 'USMALLINT',
          'unsigned_int' => 'UINTEGER'
        }.each do |column_name, expected_type|
          it "maps #{column_name} to #{expected_type}" do
            col = columns_by_name[column_name]
            expect(col).not_to be_nil, "Column '#{column_name}' not found"
            expect(col.sql_type.upcase).to eq(expected_type)
          end
        end
      end

      describe 'NOT NULL constraints' do
        let(:columns) { connection.columns(:all_types) }
        let(:columns_by_name) { columns.index_by(&:name) }

        it 'enforces NOT NULL on required columns' do
          expect(columns_by_name['record_id'].null).to be false
          expect(columns_by_name['recorded_at'].null).to be false
        end

        it 'allows NULL on optional columns' do
          expect(columns_by_name['label'].null).to be true
          expect(columns_by_name['count'].null).to be true
        end
      end
    end

    describe 'DuckLake partitioning' do
      before do
        connection.create_table(:events, id: false) do |t|
          t.bigint :event_id, null: false
          t.datetime :occurred_at, null: false
          t.string :event_type
        end
      end

      it 'sets partitioning on a table' do
        connection.set_partitioned_by(
          :events,
          ['year(occurred_at)', 'month(occurred_at)', 'day(occurred_at)']
        )

        expect(connection.table_exists?(:events)).to be true
      end

      # Note: DuckLake does not support removal of partitioning after you set it.
      # Partitioning is a one-way operation.
      # To change partitioning, recreate the table.

      it 'reflects partitioning in schema dumps' do
        connection.set_partitioned_by(
          :events,
          ['year(occurred_at)', 'month(occurred_at)']
        )

        require 'stringio'
        stream = StringIO.new
        ActiveRecord::SchemaDumper.ignore_tables = [/^ducklake_/]
        ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection_pool, stream)
        schema = stream.string

        # The dumped schema includes the partition expressions.
        expect(schema).to include('set_partitioned_by "events"')
        expect(schema).to include('year(occurred_at)')
        expect(schema).to include('month(occurred_at)')
      end
    end

    describe 'DuckLake options' do
      it 'sets parquet_version option' do
        expect { connection.set_ducklake_option('parquet_version', '2') }.not_to raise_error
      end

      it 'sets parquet_compression option' do
        expect { connection.set_ducklake_option('parquet_compression', 'zstd') }.not_to raise_error
      end
    end

    describe 'type_to_sql conversions' do
      # Check that type_to_sql converts each Rails type to the correct DuckDB SQL type.
      {
        bigint: 'BIGINT',
        integer: 'INTEGER',
        float: 'REAL',
        boolean: 'BOOLEAN',
        string: 'VARCHAR',
        datetime: 'TIMESTAMP',
        date: 'DATE',
        time: 'TIME',
        binary: 'BLOB',
        uuid: 'UUID',
        tinyint: 'TINYINT',
        smallint: 'SMALLINT',
        hugeint: 'HUGEINT',
        utinyint: 'UTINYINT',
        usmallint: 'USMALLINT',
        uinteger: 'UINTEGER',
        ubigint: 'UBIGINT',
        uhugeint: 'UHUGEINT',
        interval: 'INTERVAL'
      }.each do |rails_type, expected_sql|
        it "converts #{rails_type} to #{expected_sql}" do
          result = connection.type_to_sql(rails_type)
          expect(result).to eq(expected_sql)
        end
      end

      it 'converts decimal with precision and scale' do
        result = connection.type_to_sql(:decimal, precision: 9, scale: 6)
        expect(result).to eq('DECIMAL(9,6)')
      end

      it 'converts string with limit' do
        result = connection.type_to_sql(:string, limit: 100)
        expect(result).to eq('VARCHAR(100)')
      end

      it 'converts double to DOUBLE' do
        result = connection.type_to_sql(:double)
        expect(result).to eq('DOUBLE')
      end

      it 'converts real to REAL' do
        result = connection.type_to_sql(:real)
        expect(result).to eq('REAL')
      end
    end

    describe 'full migration workflow (create, alter, drop)' do
      it 'supports the full table lifecycle' do
        # Create
        connection.create_table(:workflow_test, id: false) do |t|
          t.bigint :id
          t.string :name
        end
        expect(connection.table_exists?(:workflow_test)).to be true

        # Add column
        connection.add_column(:workflow_test, :created_at, :datetime)
        columns = connection.columns(:workflow_test)
        expect(columns.map(&:name)).to include('created_at')

        # Drop
        connection.drop_table(:workflow_test)
        expect(connection.table_exists?(:workflow_test)).to be false
      end
    end

    # DuckLake has no indexes. It also has no PRIMARY KEY constraint.
    # The adapter omits the constraint for the default id column.
    # An explicit +id:+ type takes a different route: Rails' ordinary column path.
    # That path adds the constraint from the :primary_key column option.
    # DuckLake then rejects the whole CREATE TABLE statement.
    describe 'explicit primary key types' do
      %i[uuid string bigint integer].each do |id_type|
        it "creates a table with id: :#{id_type} and no PRIMARY KEY constraint" do
          expect { connection.create_table(:keyed, id: id_type) { |t| t.string :name } }
            .not_to raise_error

          expect(connection.table_exists?(:keyed)).to be true
          expect(connection.primary_keys(:keyed)).to be_empty
          expect(connection.columns(:keyed).map(&:name)).to include('id')
        end
      end

      # DuckLake also has no sequences ("Not implemented Error: DuckLake does not support
      # sequences"). So nothing populates the column. The application must populate it.
      it 'leaves the id column without a default' do
        connection.create_table(:keyed, id: :uuid) { |t| t.string :name }

        id_column = connection.columns(:keyed).find { |column| column.name == 'id' }
        expect(id_column.default).to be_nil
        expect(id_column.default_function).to be_nil
      end

      it 'creates no sequence for the table' do
        connection.create_table(:keyed) { |t| t.string :name }

        expect(connection.sequences).to be_empty
      end
    end

    # With no primary key, ActiveRecord quotes the nil key name into `WHERE "" = ...`. The database
    # answers with `Parser Error: zero-length delimited identifier`. This message names neither the
    # table nor the cause. So the adapter refuses the empty identifier instead. It then reports what
    # to do.
    describe 'records in a table with no primary key' do
      let(:model) do
        connection.create_table(:notes, force: true) { |t| t.string :body }
        Class.new(ActiveRecord::Base) do
          self.table_name = 'notes'
          def self.name = 'Note'
        end
      end

      it 'reports no primary key' do
        expect(model.primary_key).to be_nil
      end

      it 'still inserts, scans and updates as a set' do
        model.create!(body: 'a')
        model.create!(body: 'b')

        expect(model.count).to eq(2)
        expect(model.update_all(body: 'c')).to eq(2)
        expect(model.pluck(:body)).to eq(%w[c c])
        expect(model.delete_all).to eq(2)
      end

      %i[update reload destroy].each do |action|
        it "raises a message naming the missing primary key on ##{action}" do
          record = model.create!(body: 'a')
          call = action == :update ? -> { record.update!(body: 'b') } : -> { record.public_send(action) }

          expect(&call).to raise_error(ActiveRecord::ActiveRecordError, /no primary key/)
        end
      end
    end

    # An application-supplied id column gives per-record persistence in a lake. Quack mode needs the
    # same escape.
    describe 'an application-supplied primary key' do
      let(:model) do
        connection.create_table(:widgets, id: :uuid, force: true) { |t| t.string :name }
        Class.new(ActiveRecord::Base) do
          self.table_name = 'widgets'
          self.primary_key = 'id'
          def self.name = 'Widget'

          before_create { self.id ||= SecureRandom.uuid }
        end
      end

      it 'supports find, update, reload and destroy' do
        record = model.create!(name: 'a')
        expect(record.id).to be_present

        record.update!(name: 'b')
        expect(model.find(record.id).name).to eq('b')
        expect(record.reload.name).to eq('b')

        record.destroy
        expect(model.count).to eq(0)
      end
    end
  end
end
