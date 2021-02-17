defmodule Indexed.View do
  @moduledoc """
  A data structure about a view, held in ETS.

  While prefilters may be defined statically when warming an index, views
  also define prefilters, but they are tailor-made result sets which can be
  created and destroyed throughout the `t:Indexed.t/0` lifecycle.
  """
  alias __MODULE__

  defstruct [:maintain_unique, :prefilter, :filter]

  @type t :: %View{
          maintain_unique: [atom],
          prefilter: Indexed.prefilter(),
          filter: Indexed.filter()
        }

  @typedoc """
  A fingerprint is by taking the relevant parameters which were used to
  construct the prefilter (if applicable) and filter function. This string is
  used to identify the particular view in the map returned by under the key
  named by `Indexed.view_key/1`.
  """
  @type fingerprint :: String.t()

  @doc """
  Create a unique identifier string for `params`.

  This is not used internally, but is intended as a useful tool for a caller
  for deciding whether to use an existing view or create a new one. It is
  expected that `params` would be used to create the prefilter and/or filter
  function.
  """
  @spec fingerprint(keyword | map) :: String.t()
  def fingerprint(params) do
    string =
      params
      |> Keyword.new()
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn
        {k, v} when is_binary(v) or is_atom(v) -> "#{k}.#{v}"
        {k, v} -> "#{k}.#{inspect(v)}"
      end)
      |> Enum.join(":")

    :sha256
    |> :crypto.hash(string)
    |> Base.encode16()
    |> String.downcase()
    |> String.slice(0, 24)
  end
end
