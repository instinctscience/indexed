defmodule Indexed do
  @moduledoc """
  Tools for creating an index module.
  """
  alias __MODULE__

  @ets_opts [read_concurrency: true]

  @type data_tuple :: {:asc, atom, [record]} | {:desc, atom, [record]} | {nil, nil, [record]}

  @typep id :: any
  @typep record :: map

  # Field name and value for which separate indexes for each field should be
  # kept. Note that these are made in conjunction with `get_unique/3` and are
  # not kept in state.
  @typep prefilter :: {atom, any} | nil

  defmodule Entity do
    @moduledoc "Configuration for a type of thing to be indexed."
    defstruct fields: [], maintain_unique: [], prefilters: [], ref: nil

    @typedoc """
    * `:fields` - List of `t:field/0`s to be indexed for this entity.
    * `:maintain_unique` - List of field name atoms for which to manage unique
      values for across all records.
    * `:prefilters` - List of fields which should be prefiltered on. This means
      that separate indexes will be managed for each unique value for each of
      these fields, across all records of this entity type.
    * `:ref` - ETS table reference where records of this entity type are
      stored, keyed by id.
    * `:maintain_unique` - List of field name atoms for which a list of unique
      values will be managed. These lists can be fetched via `get_unique/3`.
      Note that prefilter fields are always included.
    """
    @type t :: %__MODULE__{
            fields: [field],
            maintain_unique: [atom],
            prefilters: [atom],
            ref: :ets.tid()
          }

    @typedoc """
    A field to be indexed. 2-element tuple has the field name, followed by a
    sorting strategy, :date or nil for simple sort.
    """
    @type field :: {atom, :date | nil}
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

  defdelegate paginate(index, entity, params), to: Indexed.Paginator

  @doc """
  For a set of entities, load data and indexes to ETS for each.

  Argument is a keyword list where entity name atoms are keys and keyword
  lists of options are values. Allowed options are as follows:

  * `:data` - List of maps (with id key) -- the data to index and cache. May
    take one of the following forms:
    * `{field, direction, list}` - data `list`, with a hint that it is already
      sorted by field (atom) and direction (:asc or :desc), `t:data_tuple/0`.
    * `list` - data list with unknown ordering; must be sorted for every field.
  * `:fields` - List of field name atoms to index by.
    * If field is a DateTime, wrap it in a tuple: `{:my_field, :date_time}`.
    * Ascending and descending will be indexed for each field.
  * `:maintain_unique` - List of field name atoms for which to manage unique
    values for across all records. Default: `[]`
  * `:prefilters` - List of fields which should be prefiltered on. This means
    that separate indexes will be managed for each unique value for each of
    these fields, across all records of this entity type. Default `[]`.
  """
  @spec warm(keyword) :: struct
  def warm(args) do
    index_ref = :ets.new(:indexes, @ets_opts)

    entities =
      Map.new(args, fn {entity_name, opts} ->
        # Normalize fields.
        fields =
          Enum.map(opts[:fields] || [], fn
            {_name, :date_time} = f -> f
            name -> {name, nil}
          end)

        # Data sub-options indicate the ordering data already has on the way in.
        {_dir, _field, full_data} =
          data_tuple = resolve_data_opt(opts[:data], entity_name, fields)

        prefilters = opts[:prefilters] || []
        maintain_unique = opts[:maintain_unique] || []

        ref = :ets.new(entity_name, @ets_opts)

        # Load the records into ETS, keyed by id.
        Enum.each(full_data, &:ets.insert(ref, {&1.id, &1}))

        # Create and insert the caches for this entity: for each prefilter
        # configured, build & store indexes for each indexed field.
        # Note that `warm/1` args are just fieldnames while internally, a
        # `t:prefilter/0` refers to a `{:my_field, "possible value"}` tuple or
        # `nil` which we implicitly include, where no prefilter is applied.
        for pf_key <- [nil | prefilters] do
          warm_prefilter(index_ref, entity_name, pf_key, fields, data_tuple)
        end

        {entity_name,
         %Entity{
           fields: fields,
           maintain_unique: maintain_unique ++ prefilters,
           prefilters: prefilters,
           ref: ref
         }}
      end)

    %Indexed{entities: entities, index_ref: index_ref}
  end

  # Normalize `warm/1`'s data option.
  @spec resolve_data_opt({atom, atom, [record]} | [record] | nil, atom, [Entity.field()]) ::
          {atom, atom, [record]}
  defp resolve_data_opt({dir, name, data}, entity, fields)
       when dir in [:asc, :desc] and is_list(data) do
    # If the data hint field isn't even being indexed, raise.
    if Enum.any?(fields, &(elem(&1, 0) == name)),
      do: {dir, name, data},
      else: raise("Field #{name} is not being indexed for #{entity}.")
  end

  defp resolve_data_opt({d, _, _}, entity, _),
    do: raise("Bad input data direction for #{entity}: #{d}")

  defp resolve_data_opt(data, _, _) when is_list(data), do: {nil, nil, data}

  # If `pf_key` is nil, then we're warming the full set -- no prefilter.
  #   In this case, load indexes for each field.
  # If `pf_key` is a field name atom to prefilter on, then group the given data
  #   by that field. For each grouping, a full set of indexes for each field will
  #   be created for each unique value found. Unique values list is updated, too.
  @spec warm_prefilter(:ets.tid(), atom, atom | nil, [Entity.field()], data_tuple) :: :ok
  defp warm_prefilter(index_ref, entity_name, pf_key, fields, {d_dir, d_name, full_data}) do
    warm_for_fields = fn prefilter, data ->
      Enum.each(fields, fn field ->
        data_tuple = {d_dir, d_name, data}
        warm_index(index_ref, entity_name, prefilter, field, data_tuple)
      end)
    end

    if is_nil(pf_key) do
      warm_for_fields.(nil, full_data)
    else
      grouped = Enum.group_by(full_data, &Map.get(&1, pf_key))

      # Save list of unique values for pf_key -- having a prefilter on a field
      # implicitly includes it for `:maintain_unique`. (See `get_unique/3`)
      values = grouped |> Map.keys() |> Enum.sort()
      :ets.insert(index_ref, {unique_key(entity_name, pf_key), values})

      Enum.each(grouped, fn {pf_val, data} ->
        warm_for_fields.({pf_key, pf_val}, data)
      end)
    end
  end

  # Create the asc and desc indexes for one field.
  @spec warm_index(:ets.tid(), atom, prefilter, Entity.field(), data_tuple) :: true
  # Data direction hint matches this field -- no need to sort.
  defp warm_index(ref, entity, prefilter, {name, _sort_hint}, {data_dir, name, data}) do
    data_ids = id_list(data)

    asc_key = index_key(entity, name, :asc, prefilter)
    asc_ids = if data_dir == :asc, do: data_ids, else: Enum.reverse(data_ids)
    desc_key = index_key(entity, name, :desc, prefilter)
    desc_ids = if data_dir == :desc, do: data_ids, else: Enum.reverse(data_ids)

    :ets.insert(ref, {asc_key, asc_ids})
    :ets.insert(ref, {desc_key, desc_ids})
  end

  # Data direction hint does NOT match this field -- sorting needed.
  defp warm_index(ref, entity, prefilter, {name, sort_hint}, {_, _, data}) do
    sort_fn =
      case sort_hint do
        :date_time -> &(:lt == DateTime.compare(Map.get(&1, name), Map.get(&2, name)))
        nil -> &(Map.get(&1, name) < Map.get(&2, name))
      end

    asc_key = index_key(entity, name, :asc, prefilter)
    desc_key = index_key(entity, name, :desc, prefilter)
    asc_ids = data |> Enum.sort(sort_fn) |> id_list()

    :ets.insert(ref, {asc_key, asc_ids})
    :ets.insert(ref, {desc_key, Enum.reverse(asc_ids)})
  end

  @doc "Cache key for a given entity, field, direction, and prefilter."
  @spec index_key(atom, atom, :asc | :desc, prefilter) :: String.t()
  def index_key(entity, field_name, direction, prefilter \\ nil)

  def index_key(entity, field_name, direction, nil) do
    "#{entity}[]#{field_name}_#{direction}"
  end

  def index_key(entity, field_name, direction, {pf_key, pf_val}) do
    "#{entity}[#{pf_key}-#{pf_val}]#{field_name}_#{direction}"
  end

  @doc "Cache key holding unique values for a given entity and field."
  @spec unique_key(atom, atom) :: String.t()
  def unique_key(entity, field_name) do
    "unique_#{entity}_#{field_name}"
  end

  # Return a list of all `:id` elements from the `collection`.
  @spec id_list([record]) :: [id]
  defp id_list(collection) do
    Enum.map(collection, & &1.id)
  end

  @doc "Get an entity by id from the index."
  @spec get(t, atom, id) :: any
  def get(index, entity, id) do
    case :ets.lookup(Map.fetch!(index.entities, entity).ref, id) do
      [{^id, val}] -> val
      [] -> nil
    end
  end

  @doc "Get an index data structure."
  @spec get_index(t, atom, atom, :asc | :desc, prefilter) :: [id]
  def get_index(index, entity, field_name, direction, prefilter \\ nil) do
    get_index(index, index_key(entity, field_name, direction, prefilter))
  end

  # Get an index data structure by key (see `index_key/4`).
  @spec get_index(t, String.t()) :: [id]
  defp get_index(index, index_name) do
    case :ets.lookup(index.index_ref, index_name) do
      [{^index_name, val}] -> val
      [] -> raise "No such index: #{index_name}"
    end
  end

  @doc "Get a list of unique values for `field_name` under `entity`."
  @spec get_unique(t, atom, atom) :: [any]
  def get_unique(index, entity, field_name) do
    get_index(index, unique_key(entity, field_name))
  end

  @doc """
  Get a list of all cached entities of a certain type.

  ## Options

  * `:prefilter` - Field atom given to `warm/1`'s `:prefilter` option,
    indicating which of the entity's prepared, prefiltered list should be
    used. Default is nil - no prefilter.
  """
  @spec get_values(t, atom, atom, :asc | :desc, keyword) :: [record]
  def get_values(index, entity, order_field, order_direction, opts \\ []) do
    id_keyed_map =
      index.entities
      |> Map.fetch!(entity)
      |> Map.fetch!(:ref)
      |> :ets.tab2list()
      |> Map.new()

    index
    |> get_index(entity, order_field, order_direction, opts[:prefilter])
    |> Enum.map(&id_keyed_map[&1])
  end

  # Insert a record into the cached data. (Indexes still need updating.)
  @spec put(t, atom, record) :: true
  defp put(index, entity, %{id: id} = record) do
    :ets.insert(Map.fetch!(index.entities, entity).ref, {id, record})
  end

  # Set an index into ETS, overwriting for the key, if need be.
  @spec put_index(t, String.t(), [id]) :: true
  defp put_index(index, index_name, id_list) do
    :ets.insert(index.index_ref, {index_name, id_list})
  end

  @doc """
  Add or update a record, along with the indexes to reflect the change.

  If it is known for sure whether or not the record was previously held in
  cache, include the `already_held?` argument to speed the operation
  slightly.

  ## Options

  * `:already_held?` - `true` if it is known that the record was previously
    held in the cache; `false` if it isn't. Default is `nil` which indicates
    that it is unknown - this will be a bit slower.
  """
  @spec set_record(t, atom, record, keyword) :: :ok
  def set_record(index, entity_name, record, opts \\ []) do
    entity = Map.fetch!(index.entities, entity_name)

    # (Note, already_held? doesn't help for updating the prefilter indexes.)
    already_held? =
      case opts[:already_held?] do
        b when is_boolean(b) -> b
        _ -> not is_nil(get(index, entity_name, record.id))
      end

    put(index, entity_name, record)

    # Update the no-prefilter index.
    Enum.each(entity.fields, fn {name, _sort_hint} = field ->
      desc_key = index_key(entity_name, name, :desc)
      desc_ids = get_index(index, desc_key)

      # Remove the id from the list if it exists.
      desc_ids =
        if already_held?,
          do: Enum.reject(desc_ids, &(&1 == record.id)),
          else: desc_ids

      desc_ids = insert_by(desc_ids, record, entity_name, field, index)

      put_index(index, desc_key, desc_ids)
      put_index(index, index_key(entity_name, name, :asc), Enum.reverse(desc_ids))
    end)

    # Synthesize a data tuple based on any field we actually index.
    {some_field, _} = hd(entity.fields)
    some_field_desc_ids = get_index(index, entity_name, some_field, :desc)
    full_data = Enum.map(some_field_desc_ids, &get(index, entity_name, &1))
    data_tuple = {:desc, some_field, full_data}

    # Update the cache for each configured prefilter.
    Enum.each(entity.prefilters, fn pf_key ->
      warm_prefilter(index.index_ref, entity_name, pf_key, entity.fields, data_tuple)
    end)
  end

  # Add the id of `record` to the list of descending ids, sorting by `field`.
  @spec insert_by([id], record, atom, Entity.field(), t) :: [id]
  defp insert_by(old_desc_ids, record, entity, {name, sort_hint}, index) do
    find_fun =
      case sort_hint do
        :date_time ->
          fn id ->
            val = Map.get(get(index, entity, id), name)
            :lt == DateTime.compare(val, Map.get(record, name))
          end

        nil ->
          &(Map.get(get(index, entity, &1), name) < Map.get(record, name))
      end

    first_smaller_idx = Enum.find_index(old_desc_ids, find_fun)

    List.insert_at(old_desc_ids, first_smaller_idx || -1, record.id)
  end
end
