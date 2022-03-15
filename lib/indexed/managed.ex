defmodule Indexed.Managed do
  @moduledoc """
  Assists a GenServer in managing in-memory caches.

  By annotating the entities to be managed, `manage/4` can handle updating the
  cache for the given record and its associated records. (If associations are
  not preloaded, they will be automatically fetched.) In addition, entites with
  `:subscribe` and `:unsubscribe` functions defined will be automatically
  subscribed to and unusbscribed from as the first reference appears and the
  last one is dropped.

  ## Example

      defmodule MyApp.CarManager do
        use Indexed.Managed, repo: MyApp.Repo

        managed :cars, MyApp.Car,
          children: [passengers: {:many, :people}]

        managed :people, MyApp.Person,
          query_fn: &MyApp.person_query/1,
          subscribe: &MyApp.subscribe_to_person/1,
          unsubscribe: &MyApp.unsubscribe_from_person/1
      end

  ## Managed Macro

  For each managed entity, the name (eg. `:cars`) and module (eg. `MyApp.Car`)
  must be specified. If needed, a keyword list of options should follow.

  * `:children` - Keyword list with association fields as keys and
    `t:assoc_spec/0`s as vals. This is used when recursing in `manage/4` as
    well as when resolving. If an undeclared association is resolved,
    `Repo.get/2` will be used as a fallback.
  * `:query_fn` - Optional function which takes a queryable and returns a
    queryable. This allows for extra query logic to be added such as populating
    virtual fields. Invoked by `manage/4` when the association is needed.
  * `:id_key` - Specifies how to find the id for a record.  It can be an atom
    field name to access, a function, or a tuple in the form `{module,
    function_name}`. In the latter two cases, the record will be passed in.
    Default `:id`.
  * `:subscribe` and `:unsubscribe` - Functions which take a record's ID and
    manage the subscription. These must both be declared or neither.

  ## Tips

  If you want to `import Ecto.Query`, you'll find that its `preload/3` conflicts
  with Managed. Since Managed will use the repo as a fallback, you can exclude
  it this way.

      defmodule MyModule do
        use Indexed.Managed
        import Ecto.Query, except: [preload: 2, preload: 3]
      end
  """
  import Ecto.Query, except: [preload: 3]
  import Indexed.Helpers, only: [id: 2, normalize_preload: 1]
  alias Ecto.Association.NotLoaded
  alias Indexed.Actions.Warm
  alias Indexed.{Entity, View}
  alias Indexed.Managed.{Prepare, State}
  alias __MODULE__

  defstruct [
    :children,
    :default_path,
    :fields,
    :id_key,
    :query_fn,
    :module,
    :name,
    :prefilters,
    :tracked,
    :subscribe,
    :unsubscribe
  ]

  @typedoc """
  * `:children` - Map with assoc field name keys `t:assoc_spec/0` values.
    When this entity is managed, all children will also be managed and so on,
    recursively.
  * `:default_path` - Default associations to traverse for `manage/5`.
  * `:fields` - Used to build the index. See `Managed.Entity.t/0`.
  * `:id_key` - Used to get a record id. See `Managed.Entity.t/0`.
  * `:query_fn` - Optional function which takes a queryable and returns a
    queryable. This allows for extra query logic to be added such as populating
    virtual fields. Invoked by `manage/4` when the association is needed.
  * `:module` - The struct module which will be used for the records.
  * `:name` - Atom name of the managed entity.
  * `:prefilters` - Used to build the index. See `Managed.Entity.t/0`.
  * `:subscribe` - 1-arity function which subscribes to changes by id.
  * `:tracked` - True if another entity has a :one assoc to this. Internal.
  * `:unsubscribe` - 1-arity function which unsubscribes to changes by id.
  """
  @type t :: %Managed{
          children: children,
          default_path: path,
          fields: [atom | Entity.field()],
          id_key: id_key,
          query_fn: (Ecto.Queryable.t() -> Ecto.Queryable.t()) | nil,
          module: module,
          name: atom,
          prefilters: [atom | keyword] | nil,
          subscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil,
          tracked: boolean,
          unsubscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil
        }

  @typedoc "For convenience, state is also accepted within a wrapping map."
  @type state_or_wrapped :: %{:managed => state | nil, optional(any) => any} | state

  @typedoc "A map of field names to assoc specs."
  @type children :: %{atom => assoc_spec}

  @typedoc """
  An association spec defines an association to another entity.
  It is used to build the preload function among other things.

  * `{:one, entity_name, id_key}` - Preload function should get a record of
    `entity_name` with id matching the id found under `id_key` of the record.
  * `{:many, entity_name}` - Uses Ecto.Schema to... IO.inspect! Otherwise, works the same as the next one.
  * `{:many, entity_name, pf_key}` - Uses an order_hint default of the first
    listed field, ascending. Otherwise, works the same as the next one.
  * `{:many, entity_name, pf_key, order_hint}` - Preload function should
    use `Indexed.get_records/4`. If `pf_key` is not null, it will be replaced
    with `{pfkey, id}` where `id` is the record's id.
  * `{:repo, key, managed}` - Preload function should use `Repo.get/2` with the
    assoc's module and the id in the foreign key field for `key` in the record.
    This is the default when a child/assoc_spec isn't defined for an assoc.
  """
  @type assoc_spec_opt ::
          assoc_spec
          | {:many, entity_name :: atom}
          | {:many, entity_name :: atom, pf_key :: atom | nil}

  @type assoc_spec ::
          {:one, entity_name :: atom, id_key :: atom}
          | {:many, entity_name :: atom, pf_key :: atom | nil, order_hint}
          | {:repo, assoc_field :: atom, managed :: t}

  @type data_opt :: Warm.data_opt()

  # Path to follow when warming or updating data. Uses same format as preload.
  @type path :: atom | list

  @typep id_key :: atom | (record -> id)
  @typep add_or_rm :: :add | :rm
  @typep state :: State.t()
  @typep id :: Indexed.id()
  @typep order_hint :: Indexed.order_hint()
  @typep record :: Indexed.record()
  @typep record_or_list :: [record] | record | nil
  @typep managed_or_name :: t | atom
  @typep preloads :: atom | list

  # Used to explain the parent entity when processing its has_many relationship.
  # Either {:top, name} where name is the top-level entity name OR
  # 1. Parent entity name.
  # 2. ID of the parent.
  # 3. Field name which would have the list of children if loaded.
  @typep parent_info :: {:top, name :: atom} | {parent_name :: atom, id, path_entry :: atom} | nil

  defmacro __using__(repo: repo) do
    quote do
      import unquote(__MODULE__)
      alias unquote(__MODULE__)
      @before_compile unquote(__MODULE__)
      @managed_repo unquote(repo)
      Module.register_attribute(__MODULE__, :managed_setup, accumulate: true)

      @doc "Create a Managed state struct, without index being initialized."
      @spec init_managed_state :: Managed.State.t()
      def init_managed_state do
        State.init(__MODULE__, unquote(repo))
      end

      @doc "Returns a freshly initialized state for `Indexed.Managed`."
      @spec warm(atom, Managed.data_opt()) :: Managed.State.t()
      def warm(name, data_opt) do
        warm(name, data_opt, nil)
      end

      @doc """
      Invoke this function with (`state, entity_name, data_opt`) or
      (`entity_name, data_opt, path`).
      """
      @spec warm(
              Managed.state_or_wrapped() | atom,
              atom | Managed.data_opt(),
              Managed.data_opt() | Managed.path()
            ) ::
              Managed.state_or_wrapped()
      def warm(%{} = a, b, c), do: warm(a, b, c, nil)
      def warm(a, b, c), do: warm(init_managed_state(), a, b, c)

      @doc "Returns a freshly initialized state for `Indexed.Managed`."
      @spec warm(Managed.state_or_wrapped(), atom, Managed.data_opt(), Managed.path()) ::
              Managed.state_or_wrapped()
      # TODO - use with_state
      def warm(%{managed: nil} = state, name, data_opt, path),
        do: %{state | managed: warm(init_managed_state(), name, data_opt, path)}

      def warm(%{managed: managed} = state, name, data_opt, path),
        do: %{state | managed: warm(managed, name, data_opt, path)}

      def warm(state, name, data_opt, path),
        do: do_warm(state, name, data_opt, path)
    end
  end

  @doc "Loads data into index, populating `:tracked` and subscribing as needed."
  @spec do_warm(state, atom, data_opt, path | nil) :: state
  def do_warm(state, name, data, path \\ nil)

  def do_warm(%{index: nil, module: mod} = state, name, data, path) do
    warm_args =
      Enum.reduce(mod.__managed__(), [], fn entity, acc ->
        managed = get_managed(mod, entity)

        Keyword.put(acc, entity,
          data: [],
          fields: managed.fields,
          id_key: managed.id_key,
          prefilters: managed.prefilters
        )
      end)

    state = %{state | index: Indexed.warm(warm_args)}
    do_warm(state, name, data, path)
  end

  def do_warm(%{module: mod} = state, name, data, path) do
    # TODO - could probably make use of data_opt properly
    managed = get_managed(mod, name)
    {_, _, records} = Warm.resolve_data_opt(data, name, managed.fields)

    manage(state, name, [], records, path)
  end

  @doc "Define a managed entity."
  defmacro managed(name, module, opts \\ []) do
    quote do
      default_path =
        case unquote(opts[:default_path]) do
          nil -> []
          path -> normalize_preload(path)
        end

      @managed_setup %Managed{
        children: unquote(opts[:children] || []),
        default_path: default_path,
        fields: unquote(opts[:fields] || []),
        query_fn: unquote(opts[:query_fn]),
        id_key: unquote(opts[:id_key] || :id),
        module: unquote(module),
        name: unquote(name),
        prefilters: unquote(opts[:prefilters] || []),
        subscribe: unquote(opts[:subscribe]),
        tracked: false,
        unsubscribe: unquote(opts[:unsubscribe])
      }
    end
  end

  defmacro __before_compile__(%{module: mod}) do
    attr = &Module.get_attribute(mod, &1)
    Prepare.validate_before_compile!(mod, attr.(:managed_repo), attr.(:managed_setup))
    Module.put_attribute(mod, :managed, Prepare.rewrite_managed(attr.(:managed_setup)))
    Module.delete_attribute(mod, :managed_setup)

    quote do
      @doc "Returns a list of all managed entity names."
      @spec __managed__ :: [atom]
      def __managed__, do: Enum.map(@managed, & &1.name)

      @doc "Returns the `t:Managed.t/0` for an entity by its name or module."
      @spec __managed__(atom) :: Managed.t() | nil
      def __managed__(name), do: Enum.find(@managed, &(&1.name == name or &1.module == name))

      @doc "Returns a list of managed entity names which are tracked."
      @spec __tracked__ :: [atom]
      @tracked @managed |> Enum.filter(& &1.tracked) |> Enum.map(& &1.name)
      def __tracked__, do: @tracked

      @doc """
      Given a managed entity name or module and a field, return the preload
      function which will take a record and state and return the association
      record or list of records for `key`.
      """
      @spec __preload_fn__(atom, atom, module) :: (map, Managed.State.t() -> map | [map]) | nil
      def __preload_fn__(name, key, repo) do
        case Enum.find(@managed, &(&1.name == name or &1.module == name)) do
          %{children: %{^key => assoc_spec}} ->
            preload_fn(assoc_spec, repo)

          %{} = managed ->
            preload_fn({:repo, key, managed}, repo)

          nil ->
            nil
        end
      end
    end
  end

  @doc """
  Add, remove or update a managed record or list of them.

  The `name` entity should be declared as `managed`.

  If `state` is a map, wrapping the managed state under a `:managed` key, it
  will be used as appropriate and returned re-wrapped.
  """
  @spec manage(
          state_or_wrapped,
          managed_or_name,
          :insert | :update | record_or_list,
          record_or_list,
          path
        ) ::
          state_or_wrapped
  def manage(state, mon, orig, new, path \\ nil)

  def manage(%{managed: managed_state} = state, mon, orig, new, path) do
    %{state | managed: manage(managed_state, mon, orig, new, path)}
  end

  def manage(state, mon, orig, new, path) do
    to_list = fn
      nil -> []
      i when is_map(i) -> [i]
      i -> i
    end

    %{name: name, default_path: default_path} =
      managed =
      case mon do
        %{} -> mon
        name -> get_managed(state, name)
      end

    orig =
      case orig do
        :insert -> nil
        :update -> %{} = get(state, name, new.id)
        og -> og
      end

    path =
      case path do
        nil -> default_path
        p -> normalize_preload(p)
      end

    new_records = to_list.(new)
    orig_records = to_list.(orig)

    l = &length/1
    log("MANAGE: #{name}: #{l.(orig_records)} to #{l.(new_records)}")

    manage_top = &Enum.reduce(&2, &1, &3)
    manage_path = &do_manage_path(&1, name, &3, &2, path)

    state
    |> State.init_tmp()
    |> manage_top.(orig_records, &rm(&2, {:top, name}, managed, &1))
    |> manage_path.(orig_records, :rm)
    |> manage_top.(new_records, &add(&2, {:top, name}, managed, &1))
    |> manage_path.(new_records, :add)
    |> do_manage_finish()
  end

  @spec do_manage_finish(state) :: state
  defp do_manage_finish(%{module: mod} = state) do
    trk = Access.key(:tracking)
    put_tracking = &put_in(&1, [trk, &2, &3], &4)
    get_tmp_record = &get_in(state.tmp.records, [&1, &2])

    drop_top_rm_ids(state)
    drop_rm_ids(state)

    # TODO: If I delete a record here, I might need to delete other connected records.
    # ... shouldn't matter if manage's path param is deep enough.
    handle = fn
      # Had 0 references, now have 1+.
      st, name, id, 0, new_c when new_c > 0 ->
        log({name, id, 0, new_c}, label: "had 0, now have 1+")
        maybe_subscribe(mod, name, id)
        put(st, name, get_tmp_record.(name, id))
        put_tracking.(st, name, id, new_c)

      # Had 1+ references, now have 0.
      st, name, id, orig_c, 0 ->
        log({name, id, orig_c, 0}, label: "had some, now have 0")

        log(st.tmp.keep, label: "keeps #{name}")

        unless has_referring_many?(state, name, id) do
          log(label: "dropping #{name} #{id}")
          drop(st, name, id)
        end

        maybe_unsubscribe(mod, name, id)
        update_in(st, [trk, name], &Map.delete(&1, id))

      # Had 1+, still have 1+. If new record isn't in tmp, it is unchanged.
      st, name, id, orig_c, new_c ->
        log({name, "id #{id}", orig_c, new_c}, label: "had 1+ still have 1+")
        log(state.tmp.tracking, label: "TMP")
        log(state.tracking, label: "TRK")
        tmp_rec = get_tmp_record.(name, id)
        if tmp_rec, do: put(st, name, tmp_rec)
        put_tracking.(st, name, id, new_c)
    end

    state =
      Enum.reduce(state.tmp.tracking, state, fn {name, map}, acc ->
        Enum.reduce(map, acc, fn {id, new_count}, acc2 ->
          orig_count = tracking(state, name, id)
          handle.(acc2, name, id, orig_count, new_count)
        end)
      end)

    %{state | tmp: nil}
  end

  # Returns true if we're holding in cache
  # another record with a has_many including the record for match_id.
  @spec has_referring_many?(state, atom, id) :: boolean
  defp has_referring_many?(%{module: mod} = state, match_name, match_id) do
    Enum.any?(mod.__managed__(), fn name ->
      Enum.any?(mod.__managed__(name).children, fn
        {:many, ^match_name, prefilter_key, _} ->
          match_id == Map.fetch!(get(state, match_name, match_id), prefilter_key)

        _ ->
          false
      end)
    end)
  end

  defp drop_rm_ids(%{module: mod, tmp: %{rm_ids: rm_ids}} = state) do
    Enum.each(rm_ids, fn {parent_name, map} ->
      Enum.each(map, fn {_parent_id, map2} ->
        Enum.each(map2, fn {path_entry, ids} ->
          with %{^path_entry => {:many, name, _, _}} <- mod.__managed__(parent_name).children,
               do: Enum.each(ids, &drop(state, name, &1))
        end)
      end)
    end)
  end

  defp drop_top_rm_ids(%{tmp: %{top_rm_ids: trm_ids}} = state) do
    Enum.each(trm_ids, fn {name, ids} ->
      Enum.each(ids, &drop(state, name, &1))
    end)
  end

  # Add a record according to its managed config:
  # - If not tracked, just add to the index.
  # - If tracked, add record to tmp and also update tmp tracking data.
  @spec add(state, parent_info, t, record) :: state
  defp add(state, parent_info, %{id_key: id_key, name: name}, record) do
    id = id(record, id_key)
    record = drop_associations(record)

    case parent_info do
      {:top, ^name} ->
        log("ADD TOP (#{name}) #{inspect(record)}")
        put(state, name, record)
        drop_tmp_rm_id(state, {:top, name}, id)

      nil ->
        log("ADD undef parent (#{name}) #{inspect(record)}")
        state = put_tmp_tracking(state, name, id, &(&1 + 1))
        put_tmp_record(state, name, id, record)

      info ->
        log("ADD #{inspect(info)} (#{name}) #{inspect(record)}")
        put(state, name, record)
        drop_tmp_rm_id(state, info, id)
    end
  end

  @spec rm(state, parent_info, t, record) :: state
  defp rm(state, parent_info, %{id_key: id_key, name: name}, record) do
    id = id(record, id_key)
    cur = tmp_tracking(state, name, id)

    case {cur, parent_info} do
      {_, {:top, ^name}} ->
        log("RM TOP (#{name}) #{id}")
        put_tmp_rm_id(state, {:top, name}, id)

      {_, {_, _, _} = parent_info} ->
        log("RM (#{name}) #{id}: parent_info #{inspect(parent_info)}")
        put_tmp_rm_id(state, parent_info, id)

      {cur, _} when cur > 0 ->
        log("RM (#{name}) #{id}: new tracking #{cur - 1}")
        put_tmp_tracking(state, name, id, cur - 1)
    end
  end

  @spec drop_tmp_rm_id(state, parent_info, id) :: state
  defp drop_tmp_rm_id(state, {:top, name}, id) do
    update_in(state, [Access.key(:tmp), :top_rm_ids, name], fn
      nil -> []
      l -> l -- [id]
    end)
  end

  defp drop_tmp_rm_id(state, parent_info, id) do
    update_in_tmp_rm_id(state, parent_info, &(&1 -- [id]))
  end

  # Add an assoc id into tmp.rm_ids.
  @spec put_tmp_rm_id(state, {:top, atom} | parent_info, id) :: state
  defp put_tmp_rm_id(state, {:top, name}, id) do
    update_in(state, [Access.key(:tmp), :top_rm_ids, name], &[id | &1 || []])
  end

  defp put_tmp_rm_id(state, parent_info, id) do
    update_in_tmp_rm_id(state, parent_info, &[id | &1 || []])
  end

  defp update_in_tmp_rm_id(state, {a, b, c}, fun) do
    m = &Access.key(&1, %{})
    keys = [m.(a), m.(b), Access.key(c, [])]
    rm_ids = update_in(state.tmp.rm_ids, keys, fun)
    put_in(state, [Access.key(:tmp), :rm_ids], rm_ids)
  end

  # Handle managing associations according to path but not records themselves.
  @spec do_manage_path(state, atom, add_or_rm, [record], keyword) :: state
  defp do_manage_path(state, name, action, records, path) do
    Enum.reduce(path, state, fn {path_entry, sub_path}, acc ->
      log({path_entry, sub_path}, label: "do_manage_path path (#{action})")
      %{children: children} = get_managed(acc.module, name)
      spec = Map.fetch!(children, path_entry)

      do_manage_assoc(acc, name, path_entry, spec, action, records, sub_path)
    end)
  end

  # Manage a single association across a set of (parent) records.
  # Then recursively handle associations according to sub_path therein.
  @spec do_manage_assoc(state, atom, atom, assoc_spec, add_or_rm, [record], keyword) :: state
  # *** ONE ADD - these records have a `belongs_to :assoc_name` association.
  defp do_manage_assoc(state, name, path_entry, {:one, assoc_name, fkey}, :add, records, sub_path) do
    assoc_managed = get_managed(state, assoc_name)

    log({name, path_entry, assoc_managed.name, fkey}, label: "** ONE add")
    # log(Enum.map(records, &id(&1, assoc_managed)), label: "records")
    log(state.tracking, label: "trkk")
    log(state.tmp.tracking, label: "trkk tmp")

    {state, assoc_records} =
      Enum.reduce(records, {state, []}, fn record, {acc_state, acc_assoc_records} ->
        case Map.fetch!(record, fkey) do
          nil ->
            {acc_state, acc_assoc_records}

          assoc_id ->
            assoc =
              assoc_from_record(record, path_entry) ||
                state.repo.get(build_query(assoc_managed), assoc_id)

            {add(acc_state, nil, assoc_managed, assoc), [assoc | acc_assoc_records]}
        end
      end)

    do_manage_path(state, assoc_name, :add, assoc_records, sub_path)
  end

  # *** MANY ADD - these records have a `has_many :assoc_name` association.
  defp do_manage_assoc(
         state,
         name,
         path_entry,
         {:many, assoc_name, fkey, _},
         :add,
         records,
         sub_path
       ) do
    %{id_key: id_key, module: entity_mod} = get_managed(state.module, name)
    %{module: assoc_mod} = assoc_managed = get_managed(state.module, assoc_name)

    log({name, path_entry, assoc_mod, fkey}, label: "** MANY add")
    # log(Enum.map(records, &id(&1, assoc_managed)), label: "records")

    {assoc_records, ids} =
      Enum.reduce(records, {[], []}, fn record, {acc_assoc_records, acc_ids} ->
        case Map.fetch!(record, path_entry) do
          l when is_list(l) -> {l ++ acc_assoc_records, acc_ids}
          _ -> {acc_assoc_records, [id(record, id_key) | acc_ids]}
        end
      end)

    fkey =
      fkey ||
        :association
        |> entity_mod.__schema__(path_entry)
        |> Map.fetch!(:related_key)

    q = from x in build_query(assoc_managed), where: field(x, ^fkey) in ^ids
    from_db = state.repo.all(q)

    assoc_records = assoc_records ++ from_db

    fun = &add(&2, {name, Map.fetch!(&1, fkey), path_entry}, assoc_managed, &1)
    state = Enum.reduce(assoc_records, state, fun)

    do_manage_path(state, assoc_name, :add, assoc_records, sub_path)
  end

  # *** ONE RM - these records have a `belongs_to :assoc_name` association.
  defp do_manage_assoc(state, name, path_entry, {:one, assoc_name, fkey}, :rm, records, sub_path) do
    %{name: assoc_name} = assoc_managed = get_managed(state, assoc_name)

    log({name, path_entry, assoc_managed.name, fkey, sub_path}, label: "** ONE rm")
    # log(Enum.map(records, &id(&1, assoc_managed)), label: "records")
    # ONE rm: {:comments, :author, :users, :author_id}

    {state, assoc_records} =
      Enum.reduce(records, {state, []}, fn record, {acc_state, acc_assoc_records} ->
        case Map.fetch!(record, fkey) do
          nil ->
            {acc_state, acc_assoc_records}

          assoc_id ->
            assoc = get(acc_state, assoc_name, assoc_id)
            {rm(acc_state, nil, assoc_managed, assoc), [assoc | acc_assoc_records]}
        end
      end)

    do_manage_path(state, assoc_name, :rm, assoc_records, sub_path)
  end

  # *** MANY RM - these records have a `has_many :assoc_name` association.
  defp do_manage_assoc(
         state,
         name,
         path_entry,
         {:many, assoc_name, fkey, _},
         :rm,
         records,
         sub_path
       ) do
    %{id_key: id_key, module: module} = get_managed(state.module, name)
    assoc_managed = get_managed(state.module, assoc_name)
    fkey = fkey || get_fkey(module, path_entry)

    log({name, path_entry, {:many, assoc_name, fkey}}, label: "** MANY rm")
    # log(Enum.map(records, &id(&1, assoc_managed)), label: "records")

    {state, assoc_records} =
      Enum.reduce(records, {state, []}, fn record, {acc_state, acc_assoc_records} ->
        id = id(record, id_key)
        assoc_records = get_records(acc_state, assoc_name, {fkey, id}) || []
        fun = &rm(&2, {name, id, path_entry}, assoc_managed, &1)
        acc_state = Enum.reduce(assoc_records, acc_state, fun)

        {acc_state, assoc_records ++ acc_assoc_records}
      end)

    do_manage_path(state, assoc_name, :rm, assoc_records, sub_path)
  end

  # Get the foreign key for the `path_entry` field of `module`.
  @spec get_fkey(module, atom) :: atom
  defp get_fkey(module, path_entry) do
    module.__schema__(:association, path_entry).related_key
  end

  @spec build_query(t) :: Ecto.Queryable.t()
  defp build_query(%{module: assoc_mod, query_fn: nil}),
    do: assoc_mod

  defp build_query(%{module: assoc_mod, query_fn: query_fn}),
    do: query_fn.(assoc_mod)

  # Get the tracking (number of references) for the given entity and id.
  @spec tracking(map, atom, any) :: non_neg_integer
  defp tracking(%{tracking: tracking}, name, id),
    do: get_in(tracking, [name, id]) || 0

  # Get the tmp tracking (number of references) for the given entity and id.
  @spec tmp_tracking(map, atom, any) :: non_neg_integer
  defp tmp_tracking(%{tmp: %{tracking: tt}, tracking: t}, name, id) do
    get = &get_in(&1, [name, id])
    get.(tt) || get.(t) || 0
  end

  # Update tmp tracking. If a function is given, its return value will be used.
  # As input, the fun gets the current count, using non-tmp tracking if empty.
  @spec put_tmp_tracking(state, atom, id, non_neg_integer | (non_neg_integer -> non_neg_integer)) ::
          state
  defp put_tmp_tracking(state, name, id, num_or_fun) when is_function(num_or_fun) do
    update_in(state, [Access.key(:tmp), :tracking, name, id], fn
      nil ->
        num = Map.fetch!(state.tracking, name)[id] || 0
        num_or_fun.(num)

      num ->
        num_or_fun.(num)
    end)
  end

  defp put_tmp_tracking(state, name, id, num_or_fun),
    do: put_tmp_tracking(state, name, id, fn _ -> num_or_fun end)

  defp put_tmp_record(state, name, id, record) do
    put_in(state, [Access.key(:tmp), :records, name, id], record)
  end

  # Attempt to lift an association directly from its parent.
  @spec assoc_from_record(record, atom) :: record | nil
  defp assoc_from_record(record, path_entry) do
    case record do
      %{^path_entry => %NotLoaded{}} -> nil
      %{^path_entry => %{} = assoc} -> assoc
      _ -> nil
    end
  end

  @doc """
  Invoke `Indexed.get/3`. State may be wrapped in a map under `:managed` key.

  If `preloads` is `true`, use the entity's default path.
  """
  @spec get(state_or_wrapped, atom, id, preloads | true) :: any
  def get(state, name, id, preloads \\ nil) do
    with_state(state, fn %{index: index, module: mod} = st ->
      p = if true == preloads, do: mod.__managed__(name).default_path, else: preloads
      record = Indexed.get(index, name, id)
      if p, do: preload(record, st, p), else: record
    end)
  end

  @doc "Invoke `Indexed.put/3` with a wrapped state for convenience."
  @spec put(state_or_wrapped, atom, record) :: :ok
  def put(state, name, record) do
    with_state(state, fn %{index: index} ->
      Indexed.put(index, name, record)
    end)
  end

  @doc "Invoke `Indexed.drop/3` with a wrapped state for convenience."
  @spec drop(state_or_wrapped, atom, id) :: :ok | :error
  def drop(state, name, id) do
    with_state(state, fn %{index: index} ->
      Indexed.drop(index, name, id)
    end)
  end

  @doc "Invoke `Indexed.get_index/4` with a wrapped state for convenience."
  @spec get_index(state_or_wrapped, atom, Indexed.prefilter()) :: list | map | nil
  def get_index(state, name, prefilter \\ nil, order_hint \\ nil) do
    with_state(state, fn %{index: index} ->
      Indexed.get_index(index, name, prefilter, order_hint)
    end)
  end

  @doc "Invoke `Indexed.get_records/4` with a wrapped state for convenience."
  @spec get_records(state_or_wrapped, atom, Indexed.prefilter() | nil, order_hint | nil) ::
          [record] | nil
  def get_records(state, name, prefilter \\ nil, order_hint \\ nil) do
    with_state(state, fn %{index: index} ->
      Indexed.get_records(index, name, prefilter, order_hint)
    end)
  end

  @spec create_view(state_or_wrapped, atom, View.fingerprint(), keyword) ::
          {:ok, View.t()} | :error
  def create_view(state, name, fingerprint, opts \\ []) do
    with_state(state, fn %{index: index} ->
      Indexed.create_view(index, name, fingerprint, opts)
    end)
  end

  @spec paginate(state_or_wrapped, atom, keyword) :: Paginator.Page.t() | nil
  def paginate(state, name, params) do
    with_state(state, fn %{index: index} ->
      Indexed.paginate(index, name, params)
    end)
  end

  @doc "Invoke `Indexed.get_view/4` with a wrapped state for convenience."
  @spec get_view(state_or_wrapped, atom, View.fingerprint()) :: View.t() | nil
  def get_view(state, name, fingerprint) do
    with_state(state, fn %{index: index} ->
      Indexed.get_view(index, name, fingerprint)
    end)
  end

  # Returns a listing of entities and number of records in the cache for each.
  @spec managed_stat(state) :: keyword
  def managed_stat(state) do
    with_state(state, fn %{index: index} = st ->
      Enum.map(index.entities, fn {name, _} ->
        {name, length(get_index(st, name))}
      end)
    end)
  end

  # Invoke fun with the managed state, finding it in the :managed key if needed.
  # If fun returns a managed state and it was wrapped, rewrap it.
  @spec with_state(state_or_wrapped, (state -> any)) :: any
  defp with_state(%{managed: state} = wrapper, fun) do
    with %State{} = new_managed <- fun.(state),
         do: %{wrapper | managed: new_managed}
  end

  defp with_state(state, fun), do: fun.(state)

  # Invoke :subscribe function for the given entity id if one is defined.
  @spec maybe_subscribe(module, atom, id) :: any
  defp maybe_subscribe(mod, name, id) do
    with %{subscribe: sub} when is_function(sub) <- get_managed(mod, name),
         do: sub.(id)
  end

  # Invoke :unsubscribe function for the given entity id if one is defined.
  @spec maybe_unsubscribe(module, atom, id) :: any
  defp maybe_unsubscribe(mod, name, id) do
    with %{unsubscribe: usub} when is_function(usub) <- get_managed(mod, name),
         do: usub.(id)
  end

  defp log(val, opts \\ []) do
    if Process.get(:bb) do
      IO.inspect(val, label: opts[:label])
      # # if Process.get(:bla) do
      # str = if is_binary(val), do: val, else: inspect(val)

      # case opts[:label] do
      #   nil -> IO.puts(str)
      #   lbl -> IO.puts("#{lbl}: #{str}")
      # end
    end

    val
  end

  # Get the %Managed{} or raise an error.
  @spec get_managed(state | module, atom) :: t
  defp get_managed(%{module: mod}, name), do: get_managed(mod, name)

  defp get_managed(mod, name) do
    mod.__managed__(name) ||
      raise ":#{name} must have a managed declaration on #{inspect(mod)}."
  end

  @doc """
  Given a preload function spec, create a preload function. `key` is the key of
  the parent entity which should be filled with the child or list of children.

  See `t:preload/0`.
  """
  @spec preload_fn(assoc_spec, module) :: (map, state -> any) | nil
  def preload_fn({:one, name, key}, _repo) do
    fn record, state ->
      get(state, name, Map.get(record, key))
    end
  end

  def preload_fn({:many, name, pf_key, order_hint}, _repo) do
    fn record, state ->
      pf = if pf_key, do: {pf_key, record.id}, else: nil
      get_records(state, name, pf, order_hint) || []
    end
  end

  def preload_fn({:repo, key, %{module: module}}, repo) do
    {owner_key, related} =
      case module.__schema__(:association, key) do
        %{owner_key: k, related: r} -> {k, r}
        nil -> raise "Expected association #{key} on #{inspect(module)}."
      end

    fn record, _state ->
      with id when id != nil <- Map.get(record, owner_key),
           do: repo.get(related, id)
    end
  end

  def preload_fn(_, _), do: nil

  @doc "Preload associations recursively."
  @spec preload(map | [map] | nil, state_or_wrapped, preloads) :: [map] | map | nil
  def preload(nil, _, _), do: nil

  def preload(record_or_list, %{managed: managed}, preloads) do
    preload(record_or_list, managed, preloads)
  end

  def preload(record_or_list, state, preloads) when is_list(record_or_list) do
    Enum.map(record_or_list, &preload(&1, state, preloads))
  end

  def preload(record_or_list, %{module: mod} = state, preloads) do
    record = record_or_list

    preload = fn
      %record_mod{} = record, key ->
        fun =
          mod.__preload_fn__(record_mod, key, state.repo) ||
            raise("No preload for #{inspect(record_mod)}.#{key} under #{inspect(mod)}.")

        fun.(record, state)

      _key, nil ->
        fn _, _ -> nil end
    end

    listify = fn
      nil -> []
      pl when is_list(pl) -> pl
      pl -> [pl]
    end

    Enum.reduce(listify.(preloads), record, fn
      {key, sub_pl}, acc ->
        preloaded = acc |> preload.(key) |> preload(state, listify.(sub_pl))
        Map.put(acc, key, preloaded)

      key, acc ->
        Map.put(acc, key, preload.(acc, key))
    end)
  end

  # Unload all associations (or only `assocs`) in an ecto schema struct.
  @spec drop_associations(struct, [atom] | nil) :: struct
  defp drop_associations(%mod{} = schema, assocs \\ nil) do
    Enum.reduce(assocs || mod.__schema__(:associations), schema, fn association, schema ->
      %{schema | association => build_not_loaded(mod, association)}
    end)
  end

  @spec build_not_loaded(module, atom) :: Ecto.Association.NotLoaded.t()
  defp build_not_loaded(mod, association) do
    %{
      cardinality: cardinality,
      field: field,
      owner: owner
    } = mod.__schema__(:association, association)

    %Ecto.Association.NotLoaded{
      __cardinality__: cardinality,
      __field__: field,
      __owner__: owner
    }
  end
end
