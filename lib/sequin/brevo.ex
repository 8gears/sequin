defmodule Sequin.Brevo do
  @moduledoc """
  Adapter for the Brevo API.
  """

  use TypedStruct

  alias Sequin.Error
  alias Sequin.Brevo.EventRequest
  alias Sequin.Brevo.TransactionalRequest

  require Logger

  @base_url "https://api.brevo.com/v3"

  typedstruct module: EventRequest do
    field :email, String.t()
    field :user_id, String.t()
    field :event_name, String.t(), enforce: true
    field :event_properties, map()
    field :mailing_lists, map()
  end

  typedstruct module: TransactionalRequest do
    field :to, String.t(), enforce: true
    field :template_id, String.t(), enforce: true
    field :params, map()
  end

  defimpl Jason.Encoder, for: EventRequest do
    def encode(struct, opts) do
      struct
      |> Map.from_struct()
      |> Sequin.Map.camelize_keys()
      |> Sequin.Map.reject_nil_values()
      |> Jason.Encode.map(opts)
    end
  end

  defimpl Jason.Encoder, for: TransactionalRequest do
    def encode(struct, opts) do
      struct
      |> Map.from_struct()
      |> Sequin.Map.camelize_keys()
      |> Sequin.Map.reject_nil_values()
      |> Jason.Encode.map(opts)
    end
  end

  @doc """
  Sends an event to trigger emails in Brevo.
  """
  @spec send_event(EventRequest.t(), Keyword.t()) :: {:ok, map()} | {:error, Error.t()}
  def send_event(%EventRequest{} = params, opts \\ []) do
    req_opts = Keyword.get(opts, :req_opts, [])
    api_key = Keyword.get(opts, :api_key)

    res =
      [json: params, url: "/events/send", auth: {:bearer, api_key}]
      |> Keyword.merge(req_opts)
      |> base_req()
      |> Req.post()

    handle_response(res)
  end

  @doc """
  Sends a transactional email to a contact.
  """
  @spec send_transactional(TransactionalRequest.t(), Keyword.t()) :: {:ok, map()} | {:error, Error.t()}
  def send_transactional(%TransactionalRequest{} = params, opts \\ []) do
    api_key = Keyword.get(opts, :api_key)

    body = %{
      "params" => params.params,
      "templateId" => params.template_id,
      "to" => [%{"email" => params.to}]
    }
    Logger.info("Body: #{inspect(body)}")
    res = Req.post(@base_url <> "/smtp/email", json: body, headers: [{"api-key", api_key}])
    Logger.info("Brevo response: #{inspect(res)}")

    handle_response(res)
  end

  defp handle_response({:ok, %Req.Response{status: status, body: body}}) when status in 200..299 do
    {:ok, body}
  end

  defp handle_response({:ok, %Req.Response{status: status, body: body}}) do
    error =
      case status do
        401 -> Error.unauthorized(message: "Unauthorized: Invalid API key or insufficient permissions")
        404 -> Error.not_found(entity: :brevo_ressource, params: %{})
        _ -> Error.service(code: "brevo_error", message: "Brevo API error", service: :brevo, details: body)
      end

    {:error, error}
  end

  defp handle_response({:error, %Mint.TransportError{reason: :timeout}}) do
    {:error, Error.timeout(source: :brevo, timeout_ms: 30_000)}
  end

  defp base_req(opts) do
    [base_url: @base_url]
    |> Req.new()
    |> Req.merge(opts)
  end
end
