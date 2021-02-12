defmodule Indexed do
  @moduledoc """
  Tools for creating an index module.
  """
  alias __MODULE__

  @ets_opts [read_concurrency: true]

  @typedoc """
  A list of records, wrapped in a hint about a field and direction it's
  already sorted by.
  """
  @type data_tuple :: {:asc, atom, [record]} | {:desc, atom, [record]} | {nil, nil, [record]}

  @typedoc """
  Configuration info for a field to be indexed.

  If the field name holds a DateTime, the second element being `:date_time`
  will hint the sorting to use `DateTime.compare/2` as is necessary.

  ## Options

  * `:sort` - Indicates how the field should be sorted in ascending order:
    * `:date_time` - `DateTime.compare/2` should be used for sorting.
    * `nil` (default) - `Enum.sort/1` will be used.
  """
  @type field_config :: {field_name :: atom, opts :: keyword}

  @typedoc "Configuration info for a prefilter."
  @type prefilter_config :: {atom, opts :: keyword}

  @typedoc "Occurrences of each value (map key) under a prefilter."
  @type counts_map :: %{any => non_neg_integer}

  @typep id :: any
  @typep record :: map

  # An aligning counts_map and list of its keys.
  # Bonus: when passed into `put_uniques_bundle/5`, `nil` for the list indicates
  # that it should not be saved.
  @typep uniques_bundle :: {counts_map, list :: [any] | nil}

  # Field name and value for which separate indexes for each field should be
  # kept. Note that these are made in conjunction with `get_unique_values/4`
  # and are not kept in state.
  @typep prefilter :: {atom, any} | nil

  defmodule Entity do
    @moduledoc "Configuration for a type of thing to be indexed."
    defstruct fields: [], prefilters: [], ref: nil

    @typedoc """
    * `:fields` - List of `t:field/0`s to be indexed for this entity.
    * `:prefilters` - List of tuples indicating which fields should be
      prefiltered on. This means that separate indexes will be managed for each
      unique value for each of these fields, across all records of this entity
      type. Each two-element tuple has the field name atom and a keyword list
      of options. Allowed options:
      * `:maintain_unique` - List of field name atoms for which a list of
        unique values under the prefilter will be managed. These lists can be
        fetched via `get_unique_values/4`.
    * `:ref` - ETS table reference where records of this entity type are
      stored, keyed by id.
    """
    @type t :: %__MODULE__{
            fields: [field],
            prefilters: [Indexed.prefilter_config()],
            ref: :ets.tid()
          }

    @typedoc """
    A field to be indexed. 2-element tuple has the field name, followed by a
    sorting strategy, :date or nil for simple sort.
    """
    @type field :: {atom, keyword}
  end

  defstruct entities: %{}, index_ref: nil

  @typedoc """
  * `:entities` - Map of entity name keys to `t:Indexed.Entity.t/0`
  * `:index_ref` - ETS table reference for the indexes.
  """
  @type t :: %__MODULE__{
          entities: %{atom => Entity.t()},
          index_ref: :ets.tid()
        }

  defdelegate paginate(index, entity_name, params), to: Indexed.Paginator

  @doc """
  For a set of entities, load data and indexes to ETS for each.

  Argument is a keyword list where entity name atoms are keys and keyword
  lists of options are values. Allowed options are as follows:

  * `:data` - List of maps (with id key) -- the data to index and cache.
    Required. May take one of the following forms:
    * `{field, direction, list}` - data `list`, with a hint that it is already
      sorted by field (atom) and direction (:asc or :desc), `t:data_tuple/0`.
    * `list` - data list with unknown ordering; must be sorted for every field.
  * `:fields` - List of field name atoms to index by. At least one required.
    * If field is a DateTime, use sort: `{:my_field, sort: :date_time}`.
    * Ascending and descending will be indexed for each field.
  * `:prefilters` - List of field name atoms which should be prefiltered on.
    This means that separate indexes will be managed for each unique value for
    each of these fields, across all records of this entity type. While field
    name `nil` refers to the indexes where no prefilter is used (all records)
    and it is included by default, it may be defined in the arguments if
    further options are needed. Default `[{nil, []}]`. If options are needed,
    2-element tuples may be used in place of the atom where the the first
    element is the field name atom, and the second is a keyword list of any
    of the following options:
    * `:maintain_unique` - List of field name atoms for which a list of unique
      values under the prefilter will be managed. If the `nil` prefilter is
      defined, leave the other prefilter fields off the `:maintain_unique`
      option as these are automatically included. These lists can be fetched
      via `get_unique_values/4`.
  """
  @spec warm(keyword) :: t
  def warm(args) do
    index_ref = :ets.new(:indexes, @ets_opts)

    entities =
      Map.new(args, fn {entity_name, opts} ->
        ref = :ets.new(entity_name, @ets_opts)
        fields = resolve_fields_opt(opts[:fields], entity_name)
        prefilters = resolve_prefilters_opt(opts[:prefilters])

        {_dir, _field, full_data} =
          data_tuple = resolve_data_opt(opts[:data], entity_name, fields)

        # Load the records into ETS, keyed by id.
        Enum.each(full_data, &:ets.insert(ref, {&1.id, &1}))

        # Create and insert the caches for this entity: for each prefilter
        # configured, build & store indexes for each indexed field.
        # Internally, a `t:prefilter/0` refers to a `{:my_field, "possible
        # value"}` tuple or `nil` which we implicitly include, where no
        # prefilter is applied.
        for prefilter <- prefilters do
          warm_prefilter(index_ref, entity_name, prefilter, fields, data_tuple)
        end

        {entity_name, %Entity{fields: fields, prefilters: prefilters, ref: ref}}
      end)

    %Indexed{entities: entities, index_ref: index_ref}
  end

  # Normalize fields.
  @spec resolve_fields_opt([atom | field_config], atom) :: [field_config]
  defp resolve_fields_opt(fields, entity_name) do
    match?([_ | _], fields) || raise "At least one field to index is required on #{entity_name}."

    Enum.map(fields, fn
      {_name, _opts} = f -> f
      name when is_atom(name) -> {name, []}
    end)
  end

  # Normalize `warm/1`'s data option.
  @spec resolve_data_opt({atom, atom, [record]} | [record] | nil, atom, [Entity.field()]) ::
          {atom, atom, [record]}
  defp resolve_data_opt({dir, name, data}, entity_name, fields)
       when dir in [:asc, :desc] and is_list(data) do
    # If the data hint field isn't even being indexed, raise.
    if Enum.any?(fields, &(elem(&1, 0) == name)),
      do: {dir, name, data},
      else: raise("Field #{name} is not being indexed for #{entity_name}.")
  end

  defp resolve_data_opt({d, _, _}, entity_name, _),
    do: raise("Bad input data direction for #{entity_name}: #{d}")

  defp resolve_data_opt(data, _, _) when is_list(data), do: {nil, nil, data}

  # Normalize the prefilters option to tuples, adding `nil` prefilter if needed.
  @spec resolve_prefilters_opt([atom | keyword] | nil) :: keyword(keyword)
  defp resolve_prefilters_opt(prefilters) do
    prefilters =
      Enum.map(prefilters || [], fn
        {pf, opts} -> {pf, Keyword.take(opts, [:maintain_unique])}
        nil -> raise "Found simple `nil` prefilter. Leave it off unless opts are needed."
        pf -> {pf, []}
      end)

    if Enum.any?(prefilters, &match?({nil, _}, &1)),
      do: prefilters,
      else: [{nil, []} | prefilters]
  end

  # If `pf_key` is nil, then we're warming the full set -- no prefilter.
  #   In this case, load indexes for each field.
  # If `pf_key` is a field name atom to prefilter on, then group the given data
  #   by that field. For each grouping, a full set of indexes for each
  #   field/value pair will be created. Unique values list is updated, too.
  @spec warm_prefilter(:ets.tid(), atom, prefilter_config, [Entity.field()], data_tuple) :: :ok
  defp warm_prefilter(
         index_ref,
         entity_name,
         {pf_key, pf_opts},
         fields,
         {d_dir, d_name, full_data}
       ) do
    # Save list of unique values for each field configured by :maintain_unique.
    store_all_uniques = fn prefilter, data ->
      Enum.each(pf_opts[:maintain_unique] || [], fn field_name ->
        {counts_map, list} =
          Enum.reduce(data, {%{}, []}, fn record, {counts_map, list} ->
            val = Map.get(record, field_name)
            num = Map.get(counts_map, val, 0) + 1
            {Map.put(counts_map, val, num), [val | list]}
          end)

        bundle = {counts_map, Enum.sort(Enum.uniq(list))}
        put_uniques_bundle(bundle, index_ref, entity_name, prefilter, field_name)
      end)
    end

    warm_index = fn prefilter, field, data ->
      data_tuple = {d_dir, d_name, data}
      warm_index(index_ref, entity_name, prefilter, field, data_tuple)
    end

    if is_nil(pf_key) do
      Enum.each(fields, &warm_index.(nil, &1, full_data))

      # Store :maintain_unique fields on the nil prefilter. Other prefilters
      # imply a unique index and are handled when they are processed below.
      store_all_uniques.(nil, full_data)
    else
      grouped = Enum.group_by(full_data, &Map.get(&1, pf_key))

      # Prepare & store list of no-prefilter uniques for this field.
      # (Remember that prefilter fields imply :maintain_unique on the nil
      # prefilter since these are needed in order to know what is useful to
      # pass into `get_unique_values/4`.)
      {counts_map, list} =
        Enum.reduce(grouped, {%{}, []}, fn {pf_val, records}, {counts_map, list} ->
          {Map.put(counts_map, pf_val, length(records)), [pf_val | list]}
        end)

      bundle = {counts_map, Enum.sort(Enum.uniq(list))}
      put_uniques_bundle(bundle, index_ref, entity_name, nil, pf_key)

      # For each value found for the prefilter, create a set of indexes.
      Enum.each(grouped, fn {pf_val, data} ->
        prefilter = {pf_key, pf_val}
        Enum.each(fields, &warm_index.(prefilter, &1, data))
        store_all_uniques.(prefilter, data)
      end)
    end
  end

  # Store two indexes for unique value tracking.
  @spec put_uniques_bundle(uniques_bundle, :ets.tid(), atom, prefilter, atom) :: true
  defp put_uniques_bundle({counts_map, list}, index_ref, entity_name, prefilter, field_name) do
    if list do
      list_key = unique_values_key(entity_name, prefilter, field_name, :list)
      :ets.insert(index_ref, {list_key, list})
    end

    counts_key = unique_values_key(entity_name, prefilter, field_name, :counts)
    :ets.insert(index_ref, {counts_key, counts_map})
  end

  # Create the asc and desc indexes for one field.
  @spec warm_index(:ets.tid(), atom, prefilter, Entity.field(), data_tuple) :: true
  # Data direction hint matches this field -- no need to sort.
  defp warm_index(ref, entity_name, prefilter, {name, _sort_hint}, {data_dir, name, data}) do
    data_ids = id_list(data)

    asc_key = index_key(entity_name, name, :asc, prefilter)
    asc_ids = if data_dir == :asc, do: data_ids, else: Enum.reverse(data_ids)
    desc_key = index_key(entity_name, name, :desc, prefilter)
    desc_ids = if data_dir == :desc, do: data_ids, else: Enum.reverse(data_ids)

    :ets.insert(ref, {asc_key, asc_ids})
    :ets.insert(ref, {desc_key, desc_ids})
  end

  # Data direction hint does NOT match this field -- sorting needed.
  defp warm_index(ref, entity_name, prefilter, {name, opts}, {_, _, data}) do
    sort_fn =
      case opts[:sort] do
        :date_time -> &(:lt == DateTime.compare(Map.get(&1, name), Map.get(&2, name)))
        nil -> &(Map.get(&1, name) < Map.get(&2, name))
      end

    asc_key = index_key(entity_name, name, :asc, prefilter)
    desc_key = index_key(entity_name, name, :desc, prefilter)
    asc_ids = data |> Enum.sort(sort_fn) |> id_list()

    :ets.insert(ref, {asc_key, asc_ids})
    :ets.insert(ref, {desc_key, Enum.reverse(asc_ids)})
  end

  @doc "Cache key for a given entity, field, direction, and prefilter."
  @spec index_key(atom, atom, :asc | :desc, prefilter) :: String.t()
  def index_key(entity_name, field_name, direction, prefilter \\ nil)

  def index_key(entity_name, field_name, direction, nil) do
    "#{entity_name}[]#{field_name}_#{direction}"
  end

  def index_key(entity_name, field_name, direction, {pf_key, pf_val}) do
    "#{entity_name}[#{pf_key}=#{pf_val}]#{field_name}_#{direction}"
  end

  # Cache key holding unique values for a given entity, field, and prefilter.
  @spec unique_values_key(atom, prefilter, atom, :counts | :list) :: String.t()
  defp unique_values_key(entity_name, nil, field_name, mode) do
    "unique_#{mode}_#{entity_name}[]#{field_name}"
  end

  defp unique_values_key(entity_name, {pf_key, pf_val}, field_name, mode) do
    "unique_#{mode}_#{entity_name}[#{pf_key}=#{pf_val}]#{field_name}"
  end

  # Return a list of all `:id` elements from the `collection`.
  @spec id_list([record]) :: [id]
  defp id_list(collection) do
    Enum.map(collection, & &1.id)
  end

  @doc "Get an entity by id from the index."
  @spec get(t, atom, id) :: any
  def get(index, entity_name, id) do
    case :ets.lookup(Map.fetch!(index.entities, entity_name).ref, id) do
      [{^id, val}] -> val
      [] -> nil
    end
  end

  @doc "Get an index data structure."
  @spec get_index(t, atom, atom, :asc | :desc, prefilter) :: [id]
  def get_index(index, entity_name, field_name, direction, prefilter \\ nil) do
    get_index(index, index_key(entity_name, field_name, direction, prefilter))
  end

  # Get an index data structure by key (see `index_key/4`).
  @spec get_index(t, String.t()) :: [id]
  defp get_index(index, index_name) do
    case :ets.lookup(index.index_ref, index_name) do
      [{^index_name, val}] -> val
      [] -> raise "No such index: #{index_name}"
    end
  end

  @doc """
  For the given `prefilter`, get a list of unique values for `field_name`
  under `entity_name`.

  Use one of the following for the last argument, `mode`:

  * `:counts` - used to store a map with values as keys and the number of
    occurrences across the records as vals.
  * `:list` (default) - used to store a list of the values, sorted ascending.
  """
  @spec get_unique_values(t, atom, atom, prefilter, :counts | :list) :: counts_map | [any]
  def get_unique_values(index, entity_name, field_name, prefilter \\ nil, mode \\ :list) do
    get_index(index, unique_values_key(entity_name, prefilter, field_name, mode))
  end

  # Get counts_map and list versions of a unique values list at the same time.
  @spec get_uniques_bundle(t, atom, atom, prefilter) :: uniques_bundle
  defp get_uniques_bundle(index, entity_name, field_name, prefilter) do
    counts_map = get_unique_values(index, entity_name, field_name, prefilter, :counts)
    list = get_unique_values(index, entity_name, field_name, prefilter, :list)
    {counts_map, list}
  end

  @doc """
  Get a list of all cached entities of a certain type.

  ## Options

  * `:prefilter` - Field atom given to `warm/1`'s `:prefilter` option,
    indicating which of the entity's prepared, prefiltered list should be
    used. Default is nil - no prefilter.
  """
  @spec get_values(t, atom, atom, :asc | :desc, keyword) :: [record]
  def get_values(index, entity_name, order_field, order_direction, opts \\ []) do
    index
    |> get_index(entity_name, order_field, order_direction, opts[:prefilter])
    |> Enum.map(&get(index, entity_name, &1))
  end

  # Insert a record into the cached data. (Indexes still need updating.)
  @spec put(t, atom, record) :: true
  defp put(index, entity_name, %{id: id} = record) do
    :ets.insert(Map.fetch!(index.entities, entity_name).ref, {id, record})
  end

  @doc """
  Add or update a record, along with the indexes to reflect the change.

  If it is known for sure whether or not the record was previously held in
  cache, include the `already_held?` argument to speed the operation
  slightly.
  """
  @spec set_record(t, atom, record, keyword) :: :ok
  def set_record(%{index_ref: index_ref} = index, entity_name, record, opts \\ []) do
    entity = Map.fetch!(index.entities, entity_name)
    previous = opts[:already_held?] != false && get(index, entity_name, record.id)

    # Update the record itself (by id).
    put(index, entity_name, record)

    # nil prefilter

    Enum.each(entity.prefilters, fn
      {nil, pf_opts} ->
        nil

      {pf_key, pf_opts} ->
        {_, list} = bundle = get_uniques_bundle(index, entity_name, pf_key, nil)

        # For each unique value under pf_key...
        Enum.each(list, fn value ->
          # Get and update global (prefilter nil) uniques for the pf field.
          cond do
            previous && Map.get(previous, pf_key) == Map.get(record, pf_key) ->
              # For this prefilter key, record hasn't moved. Do nothing.
              nil

            previous && previous[pf_key] == value ->
              # Record was moved to another prefilter. Remove it from this one.
              bundle
              |> remove_unique(value)
              |> put_uniques_bundle(index_ref, entity_name, nil, pf_key)

            record[pf_key] == value ->
              # Record was moved to this prefilter. Add it.
              bundle
              |> add_unique(value)
              |> put_uniques_bundle(index_ref, entity_name, nil, pf_key)

            true ->
              nil
          end

          prefilter = {pf_key, value}

          # Update indexes for each field under the current prefilter.
          Enum.each(entity.fields, fn {field_name, _} = field ->
            asc_key = index_key(entity_name, field_name, :asc, prefilter)
            desc_key = index_key(entity_name, field_name, :desc, prefilter)
            desc_ids = get_index(index, desc_key)

            # Remove the id from the list if it exists; insert it for sure.
            desc_ids = if previous, do: desc_ids -- [record.id], else: desc_ids
            desc_ids = insert_by(desc_ids, record, entity_name, field, index)

            :ets.insert(index_ref, {desc_key, desc_ids})
            :ets.insert(index_ref, {asc_key, Enum.reverse(desc_ids)})
          end)

          # Update any configured :maintain_uniques fields for this prefilter.
          Enum.each(pf_opts[:maintain_unique] || [], fn
            field_name ->
              bundle = get_uniques_bundle(index, entity_name, field_name, prefilter)
              new_value = Map.get(record, field_name)

              new_bundle =
                if previous && Map.get(previous, field_name) != new_value,
                  do: remove_unique(bundle, Map.get(previous, field_name)),
                  else: bundle

              new_bundle
              |> add_unique(new_value)
              |> put_uniques_bundle(index_ref, entity_name, prefilter, field_name)
          end)
        end)

        # cond do
        #   previous && previous[pf_key] != record[pf_key] ->
        #     remove_unique(index_ref, entity_name, nil, pf_key, previous[pf_key], bundle)
        #     add_unique(index_ref, entity_name, nil, pf_key, record[pf_key], bundle)
        #     # prefilter = {pf_key, previous[pf_key]}

        #     # put_uniques_bundle(index_ref, entity_name, prefilter, field_name, counts_map, list)

        #     # previous && previous[pf_key] == record[pf_key] ->
        # end

        # # unless previous && previous[pf_key] == record[pf_key] do
        # # end
        # # Enum.each(entity.fields, fn {field_name, field_opts} ->
        # #   if !previous || previous[field_name] != record[field_name] do
        # #   key = index_key(entity_name, field_name,)
        # # end)
    end)

    # # Update the no-prefilter index.

    # Enum.each(entity.fields, fn {name, _sort_hint} = field ->
    #   desc_key = index_key(entity_name, name, :desc)
    #   desc_ids = get_index(index, desc_key)

    #   # Remove the id from the list if it exists.
    #   desc_ids =
    #     if previous,
    #       do: Enum.reject(desc_ids, &(&1 == record.id)),
    #       else: desc_ids

    #   desc_ids = insert_by(desc_ids, record, entity_name, field, index)

    #   put_index(index, desc_key, desc_ids)
    #   put_index(index, index_key(entity_name, name, :asc), Enum.reverse(desc_ids))
    # end)

    # # Update the prefilter indexes.
    # Enum.each(entity.prefilters, fn prefilter ->
    #   # Synthesize a data tuple based on any field we actually index.
    #   {some_field, _} = hd(entity.fields)
    #   some_field_desc_ids = get_index(index, entity_name, some_field, :desc)
    #   full_data = Enum.map(some_field_desc_ids, &get(index, entity_name, &1))
    #   data_tuple = {:desc, some_field, full_data}

    #   # Update the cache for each configured prefilter.
    #   for pf_key <- entity.prefilters do
    #     warm_prefilter(index.index_ref, entity_name, pf_key, entity.fields, data_tuple)
    #   end
    # end)
  end

  # Remove `value` from the `field_name` unique values tally for the given
  # entity/prefilter.
  @spec remove_unique(uniques_bundle, any) :: uniques_bundle
  defp remove_unique({counts_map, list}, value) do
    case Map.fetch!(counts_map, value) do
      1 -> {Map.delete(counts_map, value), list -- [value]}
      n -> {Map.put(counts_map, value, n - 1), nil}
    end
  end

  # Add a value to the uniques: in ETS and in the returned bundle.
  @spec add_unique(uniques_bundle, any) :: uniques_bundle
  defp add_unique({counts_map, list}, value) do
    case counts_map[value] do
      nil ->
        first_bigger_idx = Enum.find_index(list, &(&1 > value))
        new_list = List.insert_at(list, first_bigger_idx || 0, value)
        new_counts_map = Map.put(counts_map, value, 1)
        {new_counts_map, new_list}

      orig_count ->
        {Map.put(counts_map, value, orig_count + 1), nil}
    end
  end

  # Add the id of `record` to the list of descending ids, sorting by `field`.
  @spec insert_by([id], record, atom, Entity.field(), t) :: [id]
  defp insert_by(old_desc_ids, record, entity_name, {name, opts}, index) do
    find_fun =
      case opts[:sort] do
        :date_time ->
          fn id ->
            val = Map.get(get(index, entity_name, id), name)
            :lt == DateTime.compare(val, Map.get(record, name))
          end

        nil ->
          this_value = Map.get(record, name)
          &(Map.get(get(index, entity_name, &1), name) < this_value)
          # &(Map.get(get(index, entity_name, &1), name) < Map.get(record, name))
      end

    first_smaller_idx = Enum.find_index(old_desc_ids, find_fun)

    List.insert_at(old_desc_ids, first_smaller_idx || -1, record.id)
  end
end
