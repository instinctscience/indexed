defmodule Indexed.Actions.CreateView do
  @moduledoc """
  Create a view - a custom result set prefilter.
  """
  alias Indexed.Actions.Warm
  alias Indexed.View

  @doc """
  Build a view for a particular set of search results, indexed by the
  `:fields` config on `index`, identified by `fingerprint`.

  ## Options

  * `:filter` - Function which takes a record and returns true if it should be
    included in the result set. This is evaluated after the prefilter.
    Default is `nil` where all values will be accepted.
  * `:maintain_unique` - List of field name atoms for which a list of unique
    values under the prefilter will be managed. These lists can be fetched via
    `Indexed.get_uniques_list/4` and `Indexed.get_uniques_map/4`.
  * `:prefilter` - Selects a pre-partitioned section of the full data of
    `entity_name`. The filter function will be applied onto this in order to
    arrive at the view's data set. See `t:Indexed.prefilter/0`.
  """
  @spec run(Indexed.t(), atom, View.fingerprint(), keyword) :: View.t() | nil
  def run(index, entity_name, fingerprint, opts \\ []) do
    entity = Map.fetch!(index.entities, entity_name)
    filter = opts[:filter]
    prefilter = opts[:prefilter]
    maintain_unique = opts[:maintain_unique] || []

    # Get current view map to ensure we're not creating an existing one.
    views_key = Indexed.views_key(entity_name)
    views = Indexed.get_index(index, views_key, %{})

    # Any pre-sorted field will do. At least we won't need to sort this one.
    {order_field, _} = hd(entity.fields)

    with ids when is_list(ids) <-
           Indexed.get_index(index, entity_name, prefilter, order_field, :asc) do
      {view_ids, counts_map_map} =
        Enum.reduce(ids, {[], %{}}, fn id, {ids, counts_map_map} ->
          record = Indexed.get(index, entity_name, id)

          if is_nil(filter) || filter.(record) do
            cmm =
              Enum.reduce(maintain_unique, counts_map_map, fn field_name, cmm ->
                value = Map.get(record, field_name)
                orig_count = get_in(counts_map_map, [field_name, value]) || 0
                put_in(cmm, Enum.map([field_name, value], &Access.key(&1, %{})), orig_count + 1)
              end)

            {[id | ids], cmm}
          else
            {ids, counts_map_map}
          end
        end)

      view_ids = Enum.reverse(view_ids)

      # Save unique value info for each field in :maintain_unique option.
      for field_name <- maintain_unique do
        counts_map = Map.get(counts_map_map, field_name, %{})
        list = counts_map |> Map.keys() |> Enum.sort()

        map_key = Indexed.uniques_map_key(entity_name, fingerprint, field_name)
        list_key = Indexed.uniques_list_key(entity_name, fingerprint, field_name)

        :ets.insert(index.index_ref, {map_key, counts_map})
        :ets.insert(index.index_ref, {list_key, list})
      end

      view_records = Enum.map(view_ids, &Indexed.get(index, entity_name, &1))

      # Save field indexes.
      for {field_name, _} = field <- entity.fields do
        sorted_ids =
          if field_name == order_field,
            do: view_ids,
            else: view_records |> Enum.sort(Warm.record_sort_fn(field)) |> Enum.map(& &1.id)

        asc_key = Indexed.index_key(entity_name, fingerprint, field_name, :asc)
        desc_key = Indexed.index_key(entity_name, fingerprint, field_name, :desc)

        :ets.insert(index.index_ref, {asc_key, sorted_ids})
        :ets.insert(index.index_ref, {desc_key, Enum.reverse(sorted_ids)})
      end

      # Add view metadata to ETS.
      view = %View{filter: filter, maintain_unique: maintain_unique, prefilter: prefilter}
      :ets.insert(index.index_ref, {views_key, Map.put(views, fingerprint, view)})

      view
    end
  end
end
