defmodule Sequin.Redis do
  @moduledoc """
  Redis client module for Sequin's internal Redis operations.

  This module provides a connection pool to Redis that is supervised and
  automatically reconnects when connections are lost.
  """
  alias __MODULE__
  alias Sequin.Error
  alias Sequin.Error.ServiceError
  alias Sequin.Redis.RedisClient
  alias Sequin.Statsd

  require Logger

  @type command :: [any()]
  @type redis_value :: binary() | integer() | nil | [redis_value()]
  @type pipeline_return_value :: redis_value() | ServiceError.t()
  @type command_opt :: {:query_name, String.t()}

  # Supervisor for Redis connections
  defmodule Supervisor do
    @moduledoc false
    use Elixir.Supervisor

    def start_link(opts) do
      Elixir.Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
    end

    @impl true
    def init(_opts) do
      Sequin.Redis.set_redis_client()

      # For cluster mode, eredis_cluster handles connections differently
      # For single mode, we use ConnectionWorkers that supervise individual eredis connections
      children =
        case Sequin.Redis.redis_client() do
          Sequin.Redis.ClusterClient ->
            # Cluster mode: connect via eredis_cluster (original behavior)
            Sequin.Redis.init_cluster_connections()
            []

          Sequin.Redis.Client ->
            # Single mode: use supervised ConnectionWorkers
            for index <- 0..(Sequin.Redis.pool_size() - 1) do
              {Sequin.Redis.ConnectionWorker, index: index}
            end
        end

      Elixir.Supervisor.init(children, strategy: :one_for_one)
    end
  end

  # Worker that wraps a single Redis connection
  defmodule ConnectionWorker do
    @moduledoc false
    use GenServer

    require Logger

    def start_link(opts) do
      index = Keyword.fetch!(opts, :index)
      GenServer.start_link(__MODULE__, index, name: via_tuple(index))
    end

    def child_spec(opts) do
      index = Keyword.fetch!(opts, :index)

      %{
        id: {__MODULE__, index},
        start: {__MODULE__, :start_link, [opts]},
        restart: :permanent,
        type: :worker
      }
    end

    defp via_tuple(index), do: {:via, Registry, {Sequin.Redis.Registry, index}}

    @impl GenServer
    def init(index) do
      Process.flag(:trap_exit, true)
      {:ok, %{index: index, conn: nil}, {:continue, :connect}}
    end

    @impl GenServer
    def handle_continue(:connect, %{index: index} = state) do
      case start_connection(index) do
        {:ok, conn} ->
          Logger.debug("[Redis.ConnectionWorker] Connected to Redis", index: index)
          {:noreply, %{state | conn: conn}}

        {:error, reason} ->
          Logger.error("[Redis.ConnectionWorker] Failed to connect to Redis, retrying...",
            index: index,
            error: inspect(reason)
          )

          # Retry after a delay
          Process.send_after(self(), :reconnect, :timer.seconds(1))
          {:noreply, state}
      end
    end

    @impl GenServer
    def handle_info(:reconnect, %{index: index} = state) do
      case start_connection(index) do
        {:ok, conn} ->
          Logger.info("[Redis.ConnectionWorker] Reconnected to Redis", index: index)
          {:noreply, %{state | conn: conn}}

        {:error, reason} ->
          Logger.warning("[Redis.ConnectionWorker] Failed to reconnect to Redis, retrying...",
            index: index,
            error: inspect(reason)
          )

          Process.send_after(self(), :reconnect, :timer.seconds(5))
          {:noreply, state}
      end
    end

    @impl GenServer
    def handle_info({:EXIT, conn, reason}, %{conn: conn, index: index} = state) do
      if reason != :normal do
        Logger.warning("[Redis.ConnectionWorker] Redis connection exited, reconnecting",
          index: index,
          reason: inspect(reason)
        )
      end

      # Schedule reconnection
      Process.send_after(self(), :reconnect, :timer.seconds(1))
      {:noreply, %{state | conn: nil}}
    end

    @impl GenServer
    def handle_info({:EXIT, _other_pid, _reason}, state) do
      # Ignore EXIT from other processes
      {:noreply, state}
    end

    defp start_connection(index) do
      opts = Sequin.Redis.connection_opts()
      name = Sequin.Redis.connection(index)

      eredis_opts =
        opts
        |> Keyword.new()
        |> Keyword.put(:name, {:local, name})
        |> Keyword.delete(:pool_size)

      case :eredis.start_link(eredis_opts) do
        {:ok, pid} -> {:ok, pid}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defmodule ClusterClient do
    @moduledoc false
    def connect(index, %{host: host, port: port} = opts) do
      opts =
        opts
        |> Map.drop([:host, :port, :database])
        |> Keyword.new()

      cluster_nodes = [{host, port}]
      :ok = :eredis_cluster.connect(Sequin.Redis.connection(index), cluster_nodes, opts)
    end

    def q(connection, command) do
      :eredis_cluster.q(connection, command)
    end

    # Redis cluster only supports pipelines where all keys map to the same node
    # We can guarantee this throughout the system by using `hash tags`
    # ie. `my-key:{some-hash-value}`
    # we have not done this yet! so we hack it here with Enum.map
    # but: this raise was left as a warning
    def qp(connection, commands) do
      if env() == :prod or length(commands) <= 3 do
        Enum.map(commands, &q(connection, &1))
      else
        raise "Redis pipeline length must be <= 3. Received #{length(commands)} commands"
      end
    end

    defp env do
      Application.get_env(:sequin, :env)
    end
  end

  defmodule Client do
    @moduledoc false
    def connect(_index, _opts) do
      # Connection is now handled by ConnectionWorker
      :ok
    end

    def q(connection, command) do
      :eredis.q(connection, command)
    end

    def qp(connection, commands) do
      :eredis.qp(connection, commands)
    end
  end

  @doc """
  Returns the child_spec for the Redis supervisor.
  This should be added to the application supervision tree.
  """
  def child_spec(opts \\ []) do
    %{
      id: __MODULE__,
      start: {__MODULE__.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Initializes cluster connections via eredis_cluster.
  Called from the Supervisor for cluster mode.
  """
  @doc false
  def init_cluster_connections do
    opts = parse_redis_connection_opts()

    for index <- 0..(pool_size() - 1) do
      ClusterClient.connect(index, opts)
    end
  rescue
    error ->
      raise "Failed to connect to Redis cluster: #{inspect(error)}"
  end

  @doc """
  For backwards compatibility. Now handled by the Supervisor.
  """
  @deprecated "Use Sequin.Redis.Supervisor instead"
  def connect_cluster do
    set_redis_client()
    # This is now a no-op as connections are managed by the Supervisor
    :ok
  end

  @spec command(command(), [opt]) :: {:ok, redis_value()} | {:error, ServiceError.t()}
        when opt: command_opt()
  def command(command, opts \\ []) do
    maybe_time(command, opts[:query_name], fn ->
      res =
        connection()
        |> redis_client().q(command)
        |> parse_result()

      case res do
        {:ok, result} ->
          {:ok, result}

        {:error, :no_connection} ->
          {:error,
           Error.service(
             service: :redis,
             code: "no_connection",
             message: "No connection to Redis"
           )}

        {:error, :timeout} ->
          {:error, Error.service(service: :redis, code: :timeout, message: "Timeout connecting to Redis")}

        {:error, error} when is_binary(error) or is_atom(error) ->
          Logger.error("Redis command failed: #{error}", error: error)

          {:error, Error.service(service: :redis, code: :command_failed, message: to_string(error))}
      end
    end)
  end

  @spec command!(command(), [opt]) :: redis_value()
        when opt: command_opt()
  def command!(command, opts \\ []) do
    maybe_time(command, opts[:query_name], fn ->
      res = connection() |> redis_client().q(command) |> parse_result()

      case res do
        {:ok, result} ->
          result

        {:error, error} ->
          raise Error.service(service: :redis, code: :command_failed, message: error)
      end
    end)
  end

  @spec pipeline([command()], [opt]) ::
          {:ok, [pipeline_return_value()]} | {:error, ServiceError.t()}
        when opt: command_opt()
  def pipeline(commands, opts \\ []) do
    maybe_time(commands, opts[:query_name], fn ->
      case redis_client().qp(connection(), commands) do
        results when is_list(results) ->
          # Convert eredis results to Redix-style results
          {:ok,
           Enum.map(results, fn
             {:ok, :undefined} ->
               nil

             {:ok, value} ->
               value

             {:error, error} when is_binary(error) ->
               Error.service(service: :redis, code: :command_failed, message: error)
           end)}

        {:error, :no_connection} ->
          {:error,
           Error.service(
             service: :redis,
             code: "no_connection",
             message: "No connection to Redis"
           )}
      end
    end)
  end

  defp parse_result({:ok, :undefined}), do: {:ok, nil}

  defp parse_result({:ok, result}) when is_list(result) do
    {:ok,
     Enum.map(result, fn
       :undefined -> nil
       other -> other
     end)}
  end

  defp parse_result(result), do: result

  defp config do
    :sequin
    |> Application.get_env(__MODULE__, [])
    |> Sequin.Keyword.reject_nils()
  end

  def connection(index \\ random_index()) do
    :"#{Redis}_#{index}"
  end

  defp random_index do
    Enum.random(0..(pool_size() - 1))
  end

  @doc false
  def pool_size do
    :sequin |> Application.fetch_env!(Redis) |> Keyword.fetch!(:pool_size)
  end

  @doc false
  def connection_opts do
    parse_redis_connection_opts() |> Keyword.new()
  end

  @sample_rate 1
  defp maybe_time([command_kind | _commands], query_name, fun) do
    command_kind =
      if is_list(command_kind), do: "pipeline-#{List.first(command_kind)}", else: command_kind

    query_name = query_name || "unnamed_query"
    {time_ms, result} = :timer.tc(fun, :millisecond)

    if Enum.random(0..99) < @sample_rate do
      Statsd.timing("sequin.redis", time_ms, tags: %{query: query_name, command_kind: command_kind})
    end

    result
  end

  defp parse_redis_connection_opts do
    {url, opts} = Keyword.pop!(config(), :url)
    opts = Map.new(opts)

    %{host: host, port: port, userinfo: userinfo, path: path} = URI.parse(url)
    opts = Map.merge(opts, %{host: to_charlist(host), port: port})

    opts =
      case path do
        "/" <> database -> Map.put(opts, :database, String.to_integer(database))
        _ -> Map.put(opts, :database, 0)
      end

    # Parse username and password from userinfo
    case userinfo do
      nil ->
        opts

      info ->
        {username, password} =
          case String.split(info, ":") do
            [user, pass] -> {user, pass}
            [pass] -> {nil, pass}
          end

        opts
        |> Map.put(:username, username)
        |> Map.put(:password, password)
    end
  end

  @doc false
  def set_redis_client do
    case parse_redis_connection_opts() do
      %{database: 0} ->
        Application.put_env(:sequin, RedisClient, ClusterClient)

      %{database: database} when database > 0 and database < 16 ->
        Application.put_env(:sequin, RedisClient, Client)
    end

    :ok
  end

  @doc false
  def redis_client do
    Application.get_env(:sequin, RedisClient)
  end
end
