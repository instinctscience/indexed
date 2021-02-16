# defmodule Indexed.CreateView do
#   @moduledoc """
#   Manages access to a particular query result.
#   """
#   alias Indexed.Actions.Warm
#   alias __MODULE__

#   @ets_opts [read_concurrency: true]

#   defstruct [:entity_name, :index, :ref]

#   @typedoc """
#   * `:entity_name` - entity name atom (eg. `:cars`)
#   * `:index` - Indexed struct, holding the `:entity_name` records
#   * `:ref` - ETS table reference for the view indexes
#   """
#   @type t :: %View{
#           entity_name: atom,
#           index: Indexed.t(),
#           ref: :ets.tid(),
#           maintain_unique: [atom]
#         }

#   @doc """
#   Build a view for a particular set of search results, indexed by the
#   `:fields` config on `index`.

#   ## Options

#   * `:filter` - function which takes a record and returns true if it should be
#     included in the result set. This is evaluated after the prefilter.
#     Default is `nil` where all values will be accepted.
#   * `:maintain_unique` - List of field name atoms for which a list of unique
#     values under the prefilter will be managed. These lists can be fetched via
#     `Indexed.get_uniques_list/4` and `Indexed.get_uniques_map/4`.
#   * `:prefilter` - selects a pre-partitioned section of the full data of
#     `entity_name`. See `t:Indexed.prefilter/0`.
#   """
#   @spec create(Indexed.t(), atom, keyword) :: t | nil
#   def create(index, entity_name, opts \\ []) do
#     entity = Map.fetch!(index.entities, entity_name)
#     filter = opts[:filter]
#     prefilter = opts[:prefilter]
#     maintain_unique = opts[:maintain_unique] || []

#     # Any pre-sorted field will do. At least we won't need to sort this one.
#     {order_field, _} = hd(entity.fields)

#     with ids when is_list(ids) <-
#            Indexed.get_index(index, entity_name, prefilter, order_field, :asc) do
#       {view_ids, counts_map_map} =
#         Enum.reduce(ids, {[], %{}}, fn id, {ids, counts_map_map} ->
#           record = Indexed.get(index, entity_name, id)

#           if is_nil(filter) || filter.(record) do
#             cmm =
#               Enum.reduce(maintain_unique, counts_map_map, fn field_name, cmm ->
#                 value = Map.get(record, field_name)
#                 orig_count = get_in(counts_map_map, [field_name, value]) || 0
#                 put_in(cmm, Enum.map([field_name, value], &Access.key(&1, %{})), orig_count + 1)
#               end)

#             {[id | ids], cmm}
#           else
#             {ids, counts_map_map}
#           end
#         end)

#       ref = :ets.new(:indexes, @ets_opts)

#       # Save unique value info for each field in :maintain_unique option.
#       for field_name <- maintain_unique do
#         counts_map = Map.get(counts_map_map, field_name, %{})
#         list = counts_map |> Map.keys() |> Enum.sort()

#         map_key = Indexed.uniques_map_key(entity_name, nil, field_name)
#         list_key = Indexed.uniques_list_key(entity_name, nil, field_name)

#         :ets.insert(ref, {map_key, counts_map})
#         :ets.insert(ref, {list_key, list})
#       end

#       view_records = Enum.map(Enum.reverse(view_ids), &Indexed.get(index, entity_name, &1))

#       # Save field indexes.
#       for {field_name, _} = field <- entity.fields do
#         sorted_ids =
#           if field_name == order_field,
#             do: view_ids,
#             else: Enum.sort(view_records, Warm.record_sort_fn(field))

#         asc_key = Indexed.index_key(entity_name, field_name, :asc, nil)
#         desc_key = Indexed.index_key(entity_name, field_name, :desc, nil)

#         :ets.insert(ref, {asc_key, sorted_ids})
#         :ets.insert(ref, {desc_key, Enum.reverse(sorted_ids)})
#       end

#       %View{entity_name: entity_name, index: index, ref: ref}
#     end
#   end

#   @doc """
#   Add or update a record, along with the indexes to reflect the change.
#   """
#   @spec put(t, Indexed.record()) :: :ok
#   def put(view, record) do
#     fields = get_in(view, Enum.map([:index, :entities, view.entity_name, :fields], &Access.key/1))

#     update
#   end

#   @doc """
#   Create a unique identifier string for `params`.

#   This is not used internally, but is intended as a useful tool for a caller
#   for deciding whether to use an existing view or create a new one. It is
#   expected that `params` would be used to create the prefilter and/or filter
#   function.
#   """
#   @spec fingerprint(keyword) :: String.t()
#   def fingerprint(params) do
#     string =
#       params
#       |> Keyword.new()
#       |> Enum.sort_by(&elem(&1, 0))
#       |> Enum.map(fn
#         {k, v} when is_binary(v) or is_atom(v) -> "#{k}.#{v}"
#         {k, v} -> "#{k}.#{inspect(v)}"
#       end)
#       |> Enum.join(":")

#     :sha256
#     |> :crypto.hash(string)
#     |> Base.encode16()
#     |> String.downcase()
#   end
# end
