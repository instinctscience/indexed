defmodule Indexed.UniquesBundle do
  @moduledoc """
  The uniques bundle is a 3-element tuple:

  1. Map of discrete values to their occurrence counts
  2. List of discrete values. (Keys of #1's map.)
  3. A boolean which is true when the list of keys has been updated and
     should be saved.
  """

  @typedoc """
  An aligning counts_map and list of its keys.

  The third argument is a boolean which will be set to `true` if the list has
  been updated and should be saved by `put_uniques_bundle/5`. The Map is
  always updated in this function.
  """
  @type t :: {counts_map, list :: [any] | nil, list_updated? :: boolean}

  @typedoc "Occurrences of each value (map key) under a prefilter."
  @type counts_map :: %{any => non_neg_integer}

  @doc "Store two indexes for unique value tracking."
  @spec put(t, :ets.tid(), atom, Indexed.prefilter(), atom) :: true
  def put(
        {counts_map, list, list_updated?},
        index_ref,
        entity_name,
        prefilter,
        field_name
      ) do
    if list_updated? do
      list_key = Indexed.unique_values_key(entity_name, prefilter, field_name, :list)
      :ets.insert(index_ref, {list_key, list})
    end

    counts_key = Indexed.unique_values_key(entity_name, prefilter, field_name, :counts)
    :ets.insert(index_ref, {counts_key, counts_map})
  end
end
