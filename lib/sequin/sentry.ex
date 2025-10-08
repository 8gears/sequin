defmodule Sequin.Sentry do
  @moduledoc false
  def init do
    env = Application.get_env(:sequin, :env)

    cond do
      System.get_env("CRASH_REPORTING_DISABLED") in ~w(true 1) ->
        Sentry.put_config(:dsn, nil)

      env == :prod ->
        # Ensure Sentry DSN was set during compile
        if is_nil(System.get_env("SENTRY_DSN")) do
          raise "SENTRY_DSN was not set at runtime. This is a bug."
        end

        :logger.add_handler(:sentry_handler, Sentry.LoggerHandler, %{})

      true ->
        :logger.add_handler(:sentry_handler, Sentry.LoggerHandler, %{})
    end
  end
end
