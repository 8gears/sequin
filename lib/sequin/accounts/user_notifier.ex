defmodule Sequin.Accounts.UserNotifier do
  @moduledoc false
  import Swoosh.Email

  alias Sequin.Mailer

  require Logger
  # Delivers the email using the application mailer.
  defp deliver(recipient, subject, template_id, params) do
    email =
      new()
      |> to(recipient)
      |> from({"Events Container Registry", "support@container-registry.com"})
      |> subject(subject)
      |> put_provider_option(:template_id, template_id)
      |> put_provider_option(:params, params)

    with {:ok, _metadata} <- Mailer.deliver(email) do
        {:ok, email}
      {:ok, email}
    end
  end

  @doc """
  Deliver instructions to confirm account.
  """
  def deliver_user_registration_confirmation(user, password_url, app_url) do
    deliver(user.email, "You have been added to our CDC platform", 22, %{
      "name" => user.name,
      "password_url" => password_url,
      "app_url" => app_url
    })
  end

  @doc """
  Deliver instructions to confirm account.
  """
  def deliver_confirmation_instructions(user, url) do
    deliver(user.email, "Please confirm your email", 24, %{
      "name" => user.name,
      "user_email" => user.email,
      "confirmation_url" => url
    })
  end

  @doc """
  Deliver instructions to reset a user password.
  """
  def deliver_reset_password_instructions(user, url) do
    deliver(user.email, "Reset your password to access your CDC account", 23, %{
      "name" => user.name,
      "user_email" => user.email,
      "reset_password_url" => url
    })
  end

  @doc """
  Deliver instructions to update a user email.
  """
  def deliver_update_email_instructions(user, url) do
    deliver(user.email, "Complete your update email request", 26, %{
      "name" => user.name,
      "user_email" => user.email,
      "update_email_url" => url
    })
  end

  def deliver_invite_to_account_instructions(send_to, inviting_user_email, account_name, url) do
    deliver(send_to, "Invite to create your CDC account", 25, %{
      "inviting_user_email" => inviting_user_email,
      "account_name" => account_name,
      "url" => url
    })
  end
end
