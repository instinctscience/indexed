defmodule Indexed.Actions.Put do
  @moduledoc "Holds internal state info during operations."
  alias Indexed.{Entity, UniquesBundle}
  alias __MODULE__

  defstruct [:bundle, :entity_name, :index, :previous, :record]

  @typedoc """
  * `:bundle` - See `t:Indexed.UniquesBundle/0`.
  * `:entity_name` - Entity name being operated on.
  * `:index` - See `t:Indexed.t/0`.
  * `:previous` - The previous version of the record. `nil` if none.
  * `:record` - The new record being added in the put operation.
  """
  @type t :: %__MODULE__{
          bundle: UniquesBundle.t() | nil,
          entity_name: atom,
          index: Indexed.t(),
          previous: Indexed.record() | nil,
          record: Indexed.record()
        }

  @doc """
  Add or update a record, along with the indexes to reflect the change.

  ## Options

  * `:new_record?` - If true, it will be assumed that the record was not
    previously held. This can speed the operation a bit.
  """
  @spec put(Indexed.t(), atom, Indexed.record(), keyword) :: :ok
  def put(index, entity_name, record, opts \\ []) do
    %{fields: fields} = entity = Map.fetch!(index.entities, entity_name)
    previous = if opts[:new_record?], do: nil, else: Indexed.get(index, entity_name, record.id)
    put = %Put{entity_name: entity_name, index: index, previous: previous, record: record}

    # Update the record itself (by id).
    :ets.insert(Map.fetch!(index.entities, entity_name).ref, {record.id, record})

    Enum.each(entity.prefilters, fn
      {nil, pf_opts} ->
        update_index_for_fields(put, nil, fields)
        update_for_maintain_unique(put, pf_opts, nil)

      {pf_key, pf_opts} ->
        # Get global (prefilter nil) uniques bundle.
        {_, list, _} = bundle = get_uniques_bundle(put, pf_key, nil)
        put = %{put | bundle: bundle}

        # For each unique value under pf_key, updated uniques.
        Enum.each(list, fn value ->
          update_uniques_for_global_prefilter(put, pf_key, value)

          prefilter = {pf_key, value}

          update_index_for_fields(put, prefilter, fields)
          update_for_maintain_unique(put, pf_opts, prefilter)
        end)
    end)
  end

  # Get and update global (prefilter nil) uniques for the pf field.
  @spec update_uniques_for_global_prefilter(t, atom, any) :: :ok
  defp update_uniques_for_global_prefilter(put, pf_key, value) do
    cond do
      put.previous && Map.get(put.previous, pf_key) == Map.get(put.record, pf_key) ->
        # For this prefilter key, record hasn't moved. Do nothing.
        nil

      put.previous && put.previous[pf_key] == value ->
        # Record was moved to another prefilter. Remove it from this one.
        put |> remove_unique(value) |> put_uniques_bundle(nil, pf_key)

      Map.get(put.record, pf_key) == value ->
        # Record was moved to this prefilter. Add it.
        put |> add_unique(value) |> put_uniques_bundle(nil, pf_key)

      true ->
        nil
    end

    :ok
  end

  # Update indexes for each field under the prefilter.
  @spec update_index_for_fields(t, Indexed.prefilter(), [Entity.field()]) :: :ok
  defp update_index_for_fields(put, prefilter, fields) do
    Enum.each(fields, fn {field_name, _} = field ->
      asc_key = Indexed.index_key(put.entity_name, field_name, :asc, prefilter)
      desc_key = Indexed.index_key(put.entity_name, field_name, :desc, prefilter)
      desc_ids = Indexed.get_index(put.index, desc_key)

      # Remove the id from the list if it exists; insert it for sure.
      desc_ids = if put.previous, do: desc_ids -- [put.record.id], else: desc_ids
      desc_ids = insert_by(put, desc_ids, field)

      :ets.insert(put.index.index_ref, {desc_key, desc_ids})
      :ets.insert(put.index.index_ref, {asc_key, Enum.reverse(desc_ids)})
    end)
  end

  # Update any configured :maintain_uniques fields for this prefilter.
  @spec update_for_maintain_unique(t, keyword, Indexed.prefilter()) :: :ok
  defp update_for_maintain_unique(put, pf_opts, prefilter) do
    Enum.each(pf_opts[:maintain_unique] || [], fn field_name ->
      put = %{put | bundle: get_uniques_bundle(put, field_name, prefilter)}
      new_value = Map.get(put.record, field_name)

      bundle =
        if put.previous && Map.get(put.previous, field_name) != new_value,
          do: remove_unique(put, Map.get(put.previous, field_name)).bundle,
          else: put.bundle

      %{put | bundle: bundle}
      |> add_unique(new_value)
      |> put_uniques_bundle(prefilter, field_name)
    end)
  end

  defp put_uniques_bundle(put, prefilter, field_name) do
    UniquesBundle.put(put.bundle, put.index.index_ref, put.entity_name, prefilter, field_name)
  end

  # Get counts_map and list versions of a unique values list at the same time.
  @spec get_uniques_bundle(t, atom, Indexed.prefilter()) :: UniquesBundle.t()
  def get_uniques_bundle(put, field_name, prefilter) do
    map = Indexed.get_uniques_map(put.index, put.entity_name, field_name, prefilter)
    list = Indexed.get_uniques_list(put.index, put.entity_name, field_name, prefilter)
    {map, list, false}
  end

  @doc """
  Remove `value` from the `field_name` unique values tally for the given
  entity/prefilter.
  """
  @spec remove_unique(t, any) :: t
  def remove_unique(%{bundle: {counts_map, list, list_updated?}} = put, value) do
    new_bundle =
      case Map.fetch!(counts_map, value) do
        1 -> {Map.delete(counts_map, value), list -- [value], true}
        n -> {Map.put(counts_map, value, n - 1), list, list_updated?}
      end

    %{put | bundle: new_bundle}
  end

  # Add a value to the uniques: in ETS and in the returned bundle.
  @spec add_unique(t, any) :: t
  def add_unique(%{bundle: {counts_map, list, list_updated?}} = put, value) do
    new_bundle =
      case counts_map[value] do
        nil ->
          first_bigger_idx = Enum.find_index(list, &(&1 > value))
          new_list = List.insert_at(list, first_bigger_idx || 0, value)
          new_counts_map = Map.put(counts_map, value, 1)
          {new_counts_map, new_list, true}

        orig_count ->
          {Map.put(counts_map, value, orig_count + 1), list, list_updated?}
      end

    %{put | bundle: new_bundle}
  end

  # Add the id of `record` to the list of descending ids, sorting by `field`.
  @spec insert_by(t, [Indexed.id()], Entity.field()) :: [Indexed.id()]
  def insert_by(put, old_desc_ids, {name, opts}) do
    find_fun =
      case opts[:sort] do
        :date_time ->
          fn id ->
            val = Map.get(Indexed.get(put.index, put.entity_name, id), name)
            :lt == DateTime.compare(val, Map.get(put.record, name))
          end

        nil ->
          this_value = Map.get(put.record, name)
          &(Map.get(Indexed.get(put.index, put.entity_name, &1), name) < this_value)
      end

    first_smaller_idx = Enum.find_index(old_desc_ids, find_fun)

    List.insert_at(old_desc_ids, first_smaller_idx || -1, put.record.id)
  end
end
