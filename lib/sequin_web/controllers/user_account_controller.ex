# lib/sequin_web/controllers/user_account_controller.ex
defmodule SequinWeb.UserAccountController do
  use SequinWeb, :controller

  alias Sequin.Accounts
  alias Sequin.Accounts.UserNotifier
  alias Sequin.YamlLoader

  require Logger

  # @spec yopass_pwd(binary) :: binary
  def yopass_pwd(password) when is_binary(password) do
    # Build the yopass command
    cmd = "echo \"#{password}\" | yopass --expiration=1w --one-time=false --api https://pwd.container-registry.com"

    {output, 0} = System.shell(cmd)
    # Parse the returned URL
    tokens = String.split(String.trim(output), "/")

    # Construct the final URL
    pwd_url = "https://container-registry.com/secrets?id=#{Enum.at(tokens, 5)}&key=#{Enum.at(tokens, 6)}"

    pwd_url
  end

  def create(conn, %{"email" => email, "name" => name, "account_name" => account_name, "is_api_token_refreshed" => is_api_token_refreshed } = params) do
    user_params =  %{
      "name" => name,
      "email" => email
    }
    Logger.info(inspect(user_params, pretty: true))

    { :ok, account } = YamlLoader.find_or_create_account(nil, %{"account" => %{"name" => account_name}})
    Logger.info(inspect(account, pretty: true))
    user = case YamlLoader.find_or_create_user(account, user_params) do
      {:created, user, password} ->
        Logger.info(inspect(user, pretty: true))
        password_url = yopass_pwd(password)
        Logger.info("Yopass URL: #{password_url}")
        UserNotifier.deliver_user_registration_confirmation(
          user,
          password_url,
          Application.fetch_env!(:sequin, :api_base_url))
        user
      {:updated, user, _} ->
        Logger.info("User already created.")
        user
    end

    Logger.info(inspect(user, pretty: true))

    case Accounts.get_account_for_user(user.id, account.id) do
      {:error, _} -> Accounts.associate_user_with_account(user, account)
      {:ok, account} -> Logger.info("Found account: #{inspect(account)}")
    end

    { :ok, token } = if is_api_token_refreshed, do: YamlLoader.update_token(account, %{"name" => "Default"}), else: YamlLoader.find_or_create_token(account, %{"name" => "Default"})
    conn
      |> put_status(:ok)
      |> json(%{data: %{"account_id" => account.id, "user_password" => user.password, "token" => token.token}, params: params})
  end
end
