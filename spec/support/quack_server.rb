# frozen_string_literal: true

require 'securerandom'
require 'socket'
require 'tmpdir'

# A real DuckDB server, serving a database over the Quack client/server protocol
#
# The integration specs in spec/active_record/connection_adapters/duckdb/quack_server_spec.rb use
# this server.
#
# Funnel mode needs a real server for its tests. The server decides the adapter's behavior in these
# ways:
# - Which statements it accepts
# - Whether session state persists
# - What a write returns
#
# This class needs the +duckdb+ CLI on PATH, version 1.5 or later. The gem's libduckdb library can
# connect to a Quack server, but it cannot serve one. +quack_serve+ must run in a process that stays
# alive.
#
# There is one server per kind. Each server starts on first use and stops when the suite ends. The
# two kinds are not interchangeable.
#
# A plain DuckDB file behind Quack stops accepting new connections once a table exists with a
# computed column default (a default value that DuckDB computes, rather than a fixed literal). This
# adapter's integer primary keys produce exactly that kind of default.
class QuackServer
  TOKEN = 'quack-spec-token'
  DATABASE = 'served'
  KINDS = %i[plain ducklake].freeze
  STARTUP_TIMEOUT = 20

  class StartupFailed < StandardError; end

  class << self
    # The running server of this kind. If the server is not up yet, this method starts it first.
    # @param kind [Symbol] :plain or :ducklake
    # @return [QuackServer]
    def for(kind)
      raise ArgumentError, "unknown kind #{kind.inspect}, expected one of #{KINDS.inspect}" unless KINDS.include?(kind)

      running[kind] ||= new(kind).start
    end

    # @return [void]
    def stop_all
      running.each_value(&:stop)
      running.clear
    end

    private

    # A class instance variable works here. RSpec runs these examples on one thread. The cache must
    # be per-class, not per-instance, so all examples share one server.
    def running
      @running ||= {} # rubocop:disable ThreadSafety/ClassInstanceVariable
    end
  end

  attr_reader :kind, :port

  # @param kind [Symbol] :plain or :ducklake
  def initialize(kind)
    @kind = kind
    @port = free_port
    @root = Dir.mktmpdir("quack-#{kind}-")
  end

  # @return [String] The URI clients attach
  def uri
    "quack:localhost:#{port}"
  end

  # The database configuration for an adapter that talks to this server
  # @return [Hash]
  def config
    {
      adapter: 'duckdb',
      database: ':memory:',
      extensions: %w[httpfs quack],
      quack: { uri: uri, token: TOKEN, database: DATABASE }
    }
  end

  # @return [self]
  def start
    ensure_cli!
    File.write(init_path, init_sql)
    # This opens a FIFO on stdin. `duckdb -init` runs the init file, then reads stdin. If stdin is
    # closed, the CLI exits immediately; it has no serve-and-block mode. The FIFO is opened
    # read-write, so it never reports EOF. This makes duckdb block on stdin for as long as this
    # process keeps its end of the FIFO open.
    File.mkfifo(fifo_path)
    @fifo = File.open(fifo_path, File::RDWR | File::NONBLOCK)
    @pid = Process.spawn('duckdb', '-init', init_path, in: @fifo, out: log_path, err: %i[child out])
    wait_until_listening
    self
  end

  # @return [void]
  def stop
    if @pid
      Process.kill('TERM', @pid)
      begin
        Timeout.timeout(5) { Process.wait(@pid) }
      rescue Timeout::Error
        Process.kill('KILL', @pid)
        Process.wait(@pid)
      rescue Errno::ECHILD, Errno::ESRCH
        nil
      end
      @pid = nil
    end
    @fifo&.close
    @fifo = nil
    FileUtils.remove_entry(@root) if File.directory?(@root)
  end

  # Runs a statement on the server itself, outside any client session.
  #
  # This method sends the statement through the FIFO, not through a client. Once a table has a
  # computed column default, the default locks out every client. At that point, the FIFO is the only
  # way in. This method exists to undo that lockout.
  #
  # @param sql [String] The statement to run, terminated or not
  # @return [void]
  def run_on_server(sql)
    @fifo.write("#{sql.chomp(";")};\n")
    @fifo.flush
    # This call does not wait for a reply. Results go to the server's log, and there is no reply
    # channel back to this method. The sleep gives the server a moment to apply the statement,
    # because callers act on the result right after this call returns.
    sleep 0.2
  end

  private

  def init_path = File.join(@root, 'server.sql')
  def fifo_path = File.join(@root, 'stdin')
  def log_path  = File.join(@root, 'server.log')

  def ensure_cli!
    return if system('duckdb', '--version', out: File::NULL, err: File::NULL)

    raise StartupFailed, <<~MESSAGE
      The duckdb CLI is not on PATH, so the Quack integration specs cannot start a server.
      Install DuckDB 1.5 or later (brew install duckdb) - the quack extension does not exist before
      1.5, and the gem's libduckdb can connect to a server but not serve one.
    MESSAGE
  end

  # Quack serves data over httpfs. Without httpfs, the port accepts connections, but every request
  # fails with HTTP 500. quack loads httpfs only when needed. So this code loads httpfs explicitly on
  # both ends.
  def init_sql
    attach = if kind == :ducklake
               "INSTALL ducklake; LOAD ducklake;\n" \
                 "ATTACH 'ducklake:#{@root}/catalog.ducklake' AS #{DATABASE} (DATA_PATH '#{@root}/data');"
             else
               "ATTACH '#{@root}/served.duckdb' AS #{DATABASE};"
             end

    <<~SQL
      SET allow_persistent_secrets = false;
      INSTALL httpfs; LOAD httpfs;
      INSTALL quack;  LOAD quack;
      SET temp_directory = '#{@root}/tmp';
      #{attach}
      USE #{DATABASE};
      SELECT listen_uri FROM quack_serve('quack:localhost:#{port}', token := '#{TOKEN}');
    SQL
  end

  # quack_serve is the last statement in the init file. So a listening port means the server is up.
  # But an error anywhere in that file kills the process instead. So this method also checks for a
  # dead child process.
  def wait_until_listening
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + STARTUP_TIMEOUT

    loop do
      raise StartupFailed, "the server exited during startup:\n#{server_log}" if exited?
      return if listening?
      raise StartupFailed, "the server did not start within #{STARTUP_TIMEOUT}s:\n#{server_log}" if
        Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.1
    end
  end

  def listening?
    TCPSocket.new('localhost', port).close
    true
  rescue SystemCallError
    false
  end

  def exited?
    !Process.wait(@pid, Process::WNOHANG).nil?
  rescue Errno::ECHILD
    true
  end

  def server_log
    File.exist?(log_path) ? File.read(log_path) : '(no output)'
  end

  def free_port
    server = TCPServer.new('localhost', 0)
    server.addr[1].tap { server.close }
  end
end
