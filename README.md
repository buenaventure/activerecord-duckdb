# Activerecord::Duckdb

This gem is a DuckDB database adapter for ActiveRecord.

## Description

Activerecord::Duckdb providers DuckDB database access for Ruby on Rails applications.

~ **NOTE:** This gem is still a work in progress, so it might not work exactly as expected just yet. Some ActiveRecord features haven’t been added and/or fully tested.

## Requirements

This gem relies on the [ruby-duckdb](https://github.com/suketa/ruby-duckdb) ruby gem as its database adapter. Thus it provides a seamless integration with the DuckDB database.

Both gems requires that you have [duckdb](https://duckdb.org). DuckDB has many installation options available that can be found on their [installation page](https://duckdb.org/docs/installation/).

```ruby
# OSx
brew install duckdb

# Most Linux distributions
curl https://install.duckdb.org | sh
```

## Installation

Install the gem and add to the application's Gemfile by executing:

```bash
bundle add "activerecord-duckdb"
```

If bundler is not being used to manage dependencies, install the gem by executing:

```bash
gem install activerecord-duckdb
```

## Usage

### Configuration

Adjust your `database.yml` file to use the duckdb adapter.

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb

test:
  adapter: duckdb
  database: db/test.duckdb

production:
  adapter: duckdb
  database: db/production.duckdb
```

Run some migrations to ensure the database is ready.

```bash
rails g model Notice name:string email:string content:string
```

```ruby
Notice.create(name: 'John Doe', email: 'john@example.com', content: 'Something happened at work today!')
Notice.find_by(email: 'john@example.com')
Notice.all
Notice.last.delete
```

~ At the moment using an in-memory database is very limited and still in development.
**NOTE:** When using a memory database, any transactional operations will be lost when the process exits.
The only reason I can think of is that you might want to use an in-memory database for testing purposes, data analysis, or some sort of quick calculations where the data is not critical.

```yaml
temporary_database:
  adapter: duckdb
  database: :memory
```

```ruby
class User < ApplicationRecord
  establish_connection(:temporary_database)
end
```

Of you can set your own database configuration in the `config/database.yml` file.
When using temporary databases you'll also have to generate your own schema on the fly rather than migrations creating them automatically.

```yml
test:
  adapter: duckdb
  database: :memory

production:
  adapter: duckdb
  database: :memory
```

### Advanced Connection Configuration

The adapter supports advanced configuration options for extensions, settings, secrets, and database attachments. These are configured in your `database.yml` file.

#### Extensions

Install and load DuckDB extensions automatically on connection:

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb
  extensions:
    - httpfs
    - postgres_scanner
    - parquet
```

#### Settings

Configure DuckDB settings. The adapter applies secure defaults which you can override:

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb
  settings:
    threads: 4
    memory_limit: '2GB'
    max_temp_directory_size: '8GB'
```

**Default Settings:**

| Setting | Default Value | Description |
|---------|---------------|-------------|
| `allow_persistent_secrets` | `false` | Disable persistent secrets for security |
| `allow_community_extensions` | `false` | Disable community extensions |
| `autoinstall_known_extensions` | `false` | Disable auto-installing extensions |
| `autoload_known_extensions` | `false` | Disable auto-loading extensions |
| `threads` | `1` | Number of threads for query execution |
| `memory_limit` | `'1GB'` | Maximum memory usage |
| `max_temp_directory_size` | `'4GB'` | Maximum temp directory size |

**Notes:**
- `allow_persistent_secrets` and `allow_community_extensions` are applied before loading extensions
- `lock_configuration = true` is automatically applied at the end to lock all settings

#### Secrets

Configure secrets for accessing external services (S3, PostgreSQL, etc.). Two styles are supported:

**Style 1: Unnamed secrets** (key is the secret type):

```yaml
development:
  adapter: duckdb
  database: ducklake
  secrets:
    postgres:
      host: localhost
      database: mydb
      user: admin
      password: secret
    s3:
      key_id: AKIAIOSFODNN7EXAMPLE
      secret: wJalrXUtnFEMI/K7MDENG
      region: us-east-1
```

**Style 2: Named secrets** (explicit `type` key, hash key becomes secret name):

```yaml
development:
  adapter: duckdb
  database: ducklake
  secrets:
    my_prod_bucket:
      type: s3
      key_id: AKIAIOSFODNN7EXAMPLE
      secret: wJalrXUtnFEMI/K7MDENG
      region: us-east-1
      scope: 's3://prod-bucket'
    my_dev_bucket:
      type: s3
      key_id: AKIAIOSFODNN7EXAMPLE2
      secret: anotherSecretKey
      region: us-west-2
      scope: 's3://dev-bucket'
```

Named secrets allow multiple secrets of the same type with different scopes.

#### Database Attachments

Attach external databases (PostgreSQL, MySQL, DuckLake, etc.):

```yaml
development:
  adapter: duckdb
  database: ducklake
  extensions:
    - postgres_scanner
    - ducklake
  secrets:
    postgres:
      host: localhost
      database: mydb
      user: admin
      password: secret
  attachments:
    - name: pg_db
      connection_string: 'postgres:'
      type: POSTGRES
    - name: ducklake
      connection_string: 'ducklake:postgres:'
      options: "DATA_PATH 's3://my-bucket', ENCRYPTED"
```

If you need to switch to a specific attached database after configuration, you can use the `use_database` option:

```yaml
development:
  adapter: duckdb
  database: db/development.duckdb
  attachments:
    - name: analytics
      connection_string: 's3://bucket/analytics.duckdb'
  use_database: analytics  # Switch to the attached database
```

#### Remote DuckDB over Quack

A `quack:` section points the connection at a DuckDB server. This server serves DuckLake over the
Quack client/server protocol. The adapter funnels every statement to that server. So the application
talks to a remote lake, but everything above the adapter stays ordinary ActiveRecord:

```yaml
production:
  adapter: duckdb
  database: ":memory:"
  extensions:
    - httpfs
    - quack
  quack:
    uri: quack:localhost
    token: <%= ENV["DUCKLAKE_QUACK_TOKEN"] %>
    database: ducklake
    disable_ssl: false # true when the server is addressed by anything but localhost
```

- `uri`: the server address, `quack:host[:port]`. The default port is 9494.
- `token`: the server's auth token, if the server requires one.
- `database`: the database to `USE` in the server session. The adapter then resolves unqualified
  names there.
- `disable_ssl`: a Quack server speaks only plain HTTP. The client picks the connection scheme from
  the hostname. For any host other than `localhost`, set this to `true`. Or put a proxy in front
  that terminates TLS.

Both `httpfs` and `quack` are required. `quack` pulls in `httpfs` lazily. So, with autoloading off, a
client that installs only `quack` fails on its first query, not at `LOAD`.

Notes and limitations:

- **No table the server serves may have a computed column default.** Attaching binds every column
  default in the server's catalog. A literal default, such as `default: false` or `default: 0`,
  binds fine. A default that needs a function or an operator does not, for example `nextval()`,
  `uuid()`, or `now()`. Such a default makes every later `ATTACH` fail, so the second connection in a
  pool cannot connect. This limit is not scoped to one database: a computed default in any database
  the server serves breaks it.

  Outside DuckLake mode, this adapter emits `DEFAULT nextval(...)` for integer primary keys. So the
  first `create_table` locks out every later connection. Avoid this one of two ways:

  - Serve a DuckLake. A DuckLake omits the default.
  - Use `id: :uuid`. This emits no default, but then nothing generates the id, so the model must
    generate it itself, for example with `before_create { self.id ||= SecureRandom.uuid }`.

  `QuackAttachmentFailed` names the offending column when this failure happens.
- **Bind parameters cannot be funneled**, because the funnel carries SQL as plain text. So the
  adapter forces `prepared_statements` off. It inlines values with its own quoting instead.
- **Savepoints are unsupported**, so nested transactions do not isolate. The adapter funnels
  transaction control too. Transaction control is per client session.

#### DuckLake + Postgres: creating and dropping the PostgreSQL database

When using DuckLake with a Postgres backend (`connection_string: 'ducklake:postgres:'` and `secrets.postgres`), `rails db:create` and `rails db:drop` will **also** create and drop the PostgreSQL database **if the `pg` gem is installed**. If the `pg` gem is not present, the Postgres database is not created or dropped (no error); create or drop it manually or add `pg` to your Gemfile.

#### DuckLake and primary keys

DuckLake has no indexes. So it has no primary key constraints, no foreign key constraints, and no
sequences. Tables get an `id` column with no constraint and no default. Every insert sets
`id = NULL`, and `Model.primary_key` is `nil`.

Bulk and analytical operations work: insert, scan, aggregate, `update_all`, `delete_all`, `pluck`,
and `find_by`. `find`, `record.update`, `record.reload`, `record.destroy`, and associations do not
work. Each of these names the reason when it fails.

For per-record persistence, supply the id from the application:

```ruby
create_table :widgets, id: :uuid do |t|
  t.string :name
end

class Widget < ApplicationRecord
  self.primary_key = 'id'
  before_create { self.id ||= SecureRandom.uuid }
end
```

From there, `find`, `update`, `reload`, `destroy`, and associations all work. Quack mode needs this
same escape. You must set `self.primary_key` by hand: the table carries no constraint for the
adapter to report.

### Sample App setup

The following steps are required to setup a sample application using the `activerecord-duckdb` gem:

1. Create a new Rails application:

```bash
rails new sample_app --database=sqlite3
```

2. Add the `activerecord-duckdb` gem to your Gemfile:

```ruby
gem 'activerecord-duckdb'
```

3. Run `bundle install` to install the gem.

4. Update the `config/database.yml` file to use the `duckdb` adapter:

```yaml
development:
  adapter: duckdb
  database: db/development.db

test:
  adapter: duckdb
  database: :memory

production:
  adapter: duckdb
  database: :memory
```

5. Generate a model for the sample application:

```bash
rails g model User name:string email:string
```

5. Run some migrations to ensure the database is ready:

```bash
rails db:create; rails db:migrate
```

6. Create some sample data:

```ruby
User.create(name: 'John Doe', email: 'john@example.com')
User.create(name: 'Jane Doe', email: 'jane@example.com')
```

7. Run some queries:

```ruby
User.all
User.find_by(email: 'john@example.com')
User.last.delete
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/tarellel/activerecord-duckdb.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
