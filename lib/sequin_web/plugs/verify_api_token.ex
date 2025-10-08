defmodule SequinWeb.Plugs.VerifyApiToken do
  @moduledoc false
  import Plug.Conn

  alias Sequin.ApiTokens
  alias Sequin.ApiTokens.ApiToken
  alias Sequin.Error
  alias Sequin.Error.NotFoundError
  alias SequinWeb.ApiFallbackPlug

  require Logger
  require URI
  @header "authorization"
  @signature_header "x-api-signature"
  @sig_prefix "sha256="

  @token_instructions """
  API tokens are generated in the Sequin console (console.sequin.io) and should be included in the Authorization header of your request. For example:

  Authorization: Bearer <your-api-token>
  """

  def header, do: @header

  def init(opts), do: opts

  defp has_signature_headers?(conn) do
    case get_req_header(conn, @signature_header) do
      [_] -> true
      _ -> false
    end
  end

  defp has_bearer_token?(conn) do
    case get_req_header(conn, @header) do
      ["Bearer " <> _] -> true
      _ -> false
    end
  end

  defp create_signature(conn) do
    method = conn.method
    request_path = conn.request_path
    body_params = conn.body_params

    body_str =
      case is_map(body_params) do
        true ->
          body_params
          |> Enum.sort_by(fn {k, _} -> k end)
          |> Enum.map(fn {k, v} ->
            "#{k}=#{to_string(v)}"
          end)
          |> Enum.join("&")

        false ->
          to_string(body_params)
      end

    # Create canonical string to sign
    signing_secret = Application.get_env(:sequin, :signing_secret)
    string_to_sign = "#{method}.#{request_path}.#{conn.query_string}.#{body_str}"

    # Compute expected signature using HMAC-SHA256
    Logger.info("string_to_sign: #{string_to_sign}")
    calculated_sig = :crypto.mac(:hmac, :sha256, signing_secret, string_to_sign)
      |> Base.encode16(case: :lower)
    @sig_prefix <> calculated_sig
  end

  defp verify_signature(conn, provided_signature) do
    # Build the payload to sign (customize based on your needs)
    expected_signature = create_signature(conn)
    Logger.info("Expected signature: " <> expected_signature)
    # Use secure comparison to prevent timing attacks
    if Plug.Crypto.secure_compare(expected_signature, provided_signature) do
      :ok
    else
      {:error, :invalid_signature}
    end
  end

  defp authenticate_with_bearer_token(conn) do
    with ["Bearer " <> token] <- get_req_header(conn, "authorization"),
         {:ok, %ApiToken{name: token_name, account_id: account_id}} <- ApiTokens.find_by_token(token) do
      Logger.metadata(account_id: account_id, api_token_name: token_name)
      assign(conn, :account_id, account_id)
    else
      {:error, %NotFoundError{entity: :api_token}} ->
        error =
          Error.unauthorized(
            message: """
            The API token you provided is invalid or has expired.
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})

      [] ->
        error =
          Error.unauthorized(
            message: """
            Please provide a valid API token in the Authorization header.

            #{@token_instructions}
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})

      [_] ->
        error =
          Error.unauthorized(
            message: """
            Please provide a valid API token in the Authorization header. Ensure your Authorization value is prefixed with "Bearer".

            #{@token_instructions}
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})

      [_, _] ->
        error =
          Error.unauthorized(
            message: """
            Please provide exactly one Authorization header in your request.

            #{@token_instructions}
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})
    end
  end

  defp authenticate_with_signature(conn) do
    with [signature] <- get_req_header(conn, @signature_header),
         :ok <- verify_signature(conn, signature) do
      assign(conn, :account_id, nil)
    else
      {:error, :invalid_signature} ->
        error =
          Error.unauthorized(
            message: "Invalid signature. Please verify your signing algorithm and secret."
          )

        ApiFallbackPlug.call(conn, {:error, error})

      _ ->
        error =
          Error.unauthorized(
            message: """
            Invalid signature authentication headers.

            #{@token_instructions}
            """
          )

        ApiFallbackPlug.call(conn, {:error, error})
    end
  end

  def call(conn, _opts) do
    cond do
      # Check for signature-based auth first
      has_signature_headers?(conn) ->
        authenticate_with_signature(conn)

      # Fall back to Bearer token auth
      has_bearer_token?(conn) ->
        authenticate_with_bearer_token(conn)
      # No authentication provided
      true ->
        error =
          Error.unauthorized(
            message: """
            Please provide authentication via Bearer token or signature.

            #{@token_instructions}
            """
          )
        ApiFallbackPlug.call(conn, {:error, error})

    end
  end
end
