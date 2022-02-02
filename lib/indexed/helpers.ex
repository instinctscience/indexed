defmodule Indexed.Helpers do
  @moduledoc "Helper functions for internal use."

  @doc """
  Get the id of the record being operated on.

  If the configured `:id_key` is a one-arity function, pass in the record to
  build the id.
  """
  @spec id_value(map, any) :: any
  def id_value(record, id_key) when is_function(id_key), do: id_key.(record)
  def id_value(record, id_key), do: Map.get(record, id_key)

  @doc """
  Get the id of the record being operated on from an action state.

  See `id_value/2`.
  """
  @spec id_value(map) :: any
  def id_value(%{entity_name: entity_name, index: %{entities: entities}, record: record}) do
    id_value(record, entities[entity_name].id_key)
  end

  @doc "Convert a field-only order hint into a tuple one."
  @spec normalize_order_hint(Indexed.order_hint()) :: [{:asc | :desc, atom}]
  def normalize_order_hint({_direction, _field} = hint), do: [hint]
  def normalize_order_hint(hint) when is_atom(hint), do: [asc: hint]

  def normalize_order_hint(hint) when is_list(hint),
    do: Enum.map(hint, &hd(normalize_order_hint(&1)))

  def normalize_order_hint(hint), do: [{:asc, hint}]

  @doc """
  Convert a preload shorthand into a predictable data structure.

  ## Examples

      iex> normalize_preload(:foo)
      [foo: []]
      iex> normalize_preload([:foo, bar: :baz])
      [foo: [], bar: [baz: []]]
  """
  @spec normalize_preload(atom | list) :: [tuple]
  def normalize_preload(preload) do
    preload
    |> is_list()
    |> if(do: preload, else: [preload])
    |> Enum.map(&do_normalize_preload/1)
  end

  @spec do_normalize_preload(atom | tuple | list) :: [tuple]
  defp do_normalize_preload(item) when is_atom(item), do: {item, []}
  defp do_normalize_preload(item) when is_list(item), do: Enum.map(item, &do_normalize_preload/1)
  defp do_normalize_preload({item, sub}) when is_atom(sub), do: {item, [{sub, []}]}
  defp do_normalize_preload({item, sub}) when is_list(sub), do: {item, do_normalize_preload(sub)}
end
