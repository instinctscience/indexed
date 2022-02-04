defmodule Indexed.Managed do
  @moduledoc """
  Assists a GenServer in managing in-memory caches.

  By annotating the entities to be managed, `manage/4` can handle updating the
  cache for the given record and its associated records. (If associations are
  not preloaded, they can be fetched via `:get_fn` by id.) In addition, entites
  with `:subscribe` and `:unsubscribe` functions defined will be automatically
  subscribed to and unusbscribed from as the first instance appears and the last
  one is dropped.

  ## Example

      defmodule MyApp.CarManager do
        use Indexed.Managed, repo: MyApp.Repo

        managed :cars, MyApp.Car,
          children: [passengers: {:many, :people, :car_id, :name}]

        managed :people, MyApp.Person,
          get_fn: &MyApp.get_person/1,
          subscribe: &MyApp.subscribe_to_person/1,
          unsubscribe: &MyApp.unsubscribe_from_person/1
      end

  ## Managed Macro

  For each managed entity, the name (eg. `:cars`) and module (eg. `MyApp.Car`)
  must be specified. If needed, a keyword list of options should follow.

  * `:children` - Keyword list with association fields as keys and
    `t:preload_spec/0`s as vals. This is used when recursing in `manage/4` as
    well as when resolving. If an undeclared association is resolved,
    `Repo.get/2` will be used as a fallback.
  * `:get_fn` - Function which takes an ID and returns the record from the
    outside. Invoked by `manage/4` when the association is needed.
  * `:id_key` - Field name atom which carries the id to index with or a
    function which accepts a record and returns the id to use. Default `:id`.
  * `:setup` - Function which takes and returns the record when `manage/4`
    begins. Useful for custom preparation steps.
  * `:subscribe` and `:unsubscribe` - Functions which take a record's ID and
    manage the subscription. These must both be declared or neither.
  """
  import Ecto.Query
  import Indexed.Helpers, only: [normalize_preload: 1]
  alias Ecto.Association.NotLoaded
  alias Indexed.Actions.Warm
  alias __MODULE__

  defmodule State do
    @moduledoc "A piece of GenServer state for Managed."
    alias __MODULE__
    defstruct [:index, :module, :repo, :tmp, :tracking]

    @typedoc """
    Data structure used to hold temporary data while running an operation.

    * `:records` - Outer map is keyed by entity name. Inner map is keyed by
      record id. Values are the records themselves. These are new records which
      may be committed to ETS at the end of the operation.
    * `:tracking` - For record ids relevant to the operation, initial values are
      copied from State and manipulated as needed within this structure.
    """
    @type tmp :: %{
            records: %{atom => %{Indexed.id() => Indexed.record()}},
            tracking: Indexed.Managed.tracking()
          }

    @typedoc """
    """
    @type t :: %State{
            index: Indexed.t() | nil,
            module: module,
            repo: module,
            tmp: tmp | nil,
            tracking: Indexed.Managed.tracking()
          }

    @doc "Returns a freshly initialized state for `Indexed.Managed`."
    @spec init(module, module) :: t
    def init(mod, repo) do
      %State{module: mod, repo: repo, tracking: init_tracking(mod)}
    end

    @doc "Returns a freshly initialized state for `Indexed.Managed`."
    @spec init_tmp(t) :: t
    def init_tmp(%{module: mod} = state) do
      records = Map.new(mod.__tracked__(), &{&1, %{}})
      %{state | tmp: %{records: records, tracking: init_tracking(mod)}}
    end

    @spec init_tracking(module) :: map
    defp init_tracking(mod), do: Map.new(mod.__tracked__(), &{&1, %{}})
  end

  @typedoc "For convenience, state is also accepted within a wrapping map."
  @type state_or_wrapped :: State.t() | %{managed: State.t()}

  @typedoc """
  A preload spec which is used to build the preload function. This function
  receives the record and the state and must return the preloaded value.
  (Note that the record is only used for reference -- it is not returned.)

  * `{:one, entity_name, id_key}` - Preload function should get a record of
    `entity_name` with id matching the id found under `id_key` of the record.
  * `{:many, entity_name, pf_key}` - Uses an order_hint default of the first
    listed field, ascending. Otherwise, works the same as the next one.
  * `{:many, entity_name, pf_key, order_hint}` - Preload function should
    use `Indexed.get_records/4`. If `pf_key` is not null, it will be replaced
    with `{pfkey, id}` where `id` is the record's id.
  * `{:repo, key, managed}` - Preload function should use `Repo.get/2` with the
    assoc's module and the id in the foreign key field for `key` in the record.
    This is the default when a child/preload_spec isn't defined for an assoc.
  """
  @type preload_spec ::
          {:one, entity_name :: atom, id_key :: atom}
          | {:many, entity_name :: atom, pf_key :: atom | nil}
          | {:many, entity_name :: atom, pf_key :: atom | nil, order_hint}
          | {:repo, assoc_field :: atom, managed :: t}

  @typedoc """
  A set of tracked entity statuses.

  An entity is tracked if it defines a `:get_fn` function.
  """
  @type tracking :: %{atom => tracking_status}

  @typedoc """
  Map of tracked record ids to occurrences throughout the records held.
  Used to manage subscriptions.
  """
  @type tracking_status :: %{id => non_neg_integer}

  @type data_opt :: Warm.data_opt()

  # Path to follow when warming or updating data. Uses same format as preload.
  @type path :: atom | [atom | keyword]

  @typep id_key :: atom | (record -> id)
  @typep state :: State.t()
  @typep id :: Indexed.id()
  @typep order_hint :: Indexed.order_hint()
  @typep record :: Indexed.record()

  defstruct [
    :children,
    :children_getters,
    :fields,
    :id_key,
    :get_fn,
    :module,
    :name,
    :prefilters,
    :setup,
    :top,
    :subscribe,
    :unsubscribe
  ]

  @typedoc """
  * `:children` - Map with assoc field name keys `t:preload_spec/0` values.
    When this entity is managed, all children will also be managed and so on,
    recursively.
  * `:id_key` - Field name atom which carries the id to index with or a
    function which accepts a record and returns the id to use. Default `:id`.
  * `:get_fn` - Function which takes a record ID and returns the record from
    the outside. Invoked by `manage/4` when a tracked record is needed.
  * `:module` - The struct module which will be used for the records.
  * `:name` - Atom name of the managed entity.
  * `:setup` - Function which takes and returns the record when `manage/4`
    begins. Useful for custom preparation steps.
  * `:subscribe` - 1-arity function which subscribes to changes by id.
  * `:top` - If true, records of this type will not be tracked.
    As a result, they will never be auto-removed.
  * `:unsubscribe` - 1-arity function which unsubscribes to changes by id.
  """
  @type t :: %Managed{
          children: %{atom => preload_spec},
          children_getters: %{atom => {module, atom}},
          fields: [atom | Indexed.Entity.field()],
          id_key: id_key,
          get_fn: (any -> map) | nil,
          module: module,
          name: atom,
          prefilters: [atom | keyword] | nil,
          setup: (map -> map) | nil,
          subscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil,
          top: boolean,
          unsubscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil
        }

  defmacro __using__(repo: repo) do
    quote do
      import unquote(__MODULE__)
      alias unquote(__MODULE__)
      @before_compile unquote(__MODULE__)
      @managed_repo unquote(repo)
      Module.register_attribute(__MODULE__, :managed, accumulate: true)

      @doc "Returns a freshly initialized state for `Indexed.Managed`."
      @spec warm(atom, Managed.data_opt(), Managed.path()) :: State.t()
      def warm(entity_name, data_opt, path) do
        state = State.init(__MODULE__, unquote(repo))
        warm(state, entity_name, data_opt, path)
      end
    end
  end

  @doc "Loads data into index, populating `:tracked` and subscribing as needed."
  @spec warm(State.t(), atom, data_opt, path) :: State.t()
  def warm(%{module: mod} = state, entity_name, data_opt, path) do
    warm_args =
      Enum.reduce(mod.__managed__(), [], fn entity, acc ->
        managed = get_managed(mod, entity)

        Keyword.put(acc, entity,
          data: [],
          fields: managed.fields,
          prefilters: managed.prefilters
        )
      end)

    # TODO - could probably make use of data_opt properly
    managed = get_managed(mod, entity_name)
    {_, _, records} = Warm.resolve_data_opt(data_opt, entity_name, managed.fields)

    %{state | index: Indexed.warm(warm_args)}
    |> State.init_tmp()
    |> do_start_add(managed, records)
    |> do_manage_path(entity_name, :add, records, normalize_preload(path))
    |> do_finish()
  end

  defp do_finish(%{module: mod} = state) do
    tk = Access.key(:tracking)
    get_record = &get_in(state.tmp.records, [&1, &2])
    put_tracking = &put_in(&1, [tk, &2, &3], &4)
    drop_tracking = fn st, n, id -> update_in(st, [tk, n], &Map.delete(&1, id)) end

    handle = fn
      st, name, id, orig_c, new_c when orig_c == 0 and new_c > 0 ->
        IO.inspect({name, id, orig_c, new_c}, label: "had none, now have some")
        maybe_subscribe(mod, name, id)
        put(st, name, get_record.(name, id) |> IO.inspect(label: "rec"))
        put_tracking.(st, name, id, new_c)

      st, name, id, orig_c, new_c when orig_c > 0 and new_c == 0 ->
        IO.inspect({name, id, orig_c, new_c}, label: "had nosome, now have none")
        maybe_unsubscribe(mod, name, id)
        drop(st, name, id)
        drop_tracking.(st, name, id)

      st, _, _, _, _ ->
        st
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

  @spec do_start_add(state, t, [record]) :: state
  defp do_start_add(state, managed, records) do
    Enum.reduce(records, state, &add(&2, managed, &1))
  end

  defp add(state, %{name: name, top: true}, record) do
    put(state, name, drop_associations(record))
    state
  end

  defp add(state, %{id_key: id_key, name: name}, record) do
    id = id(id_key, record)
    cur = tracking_tmp(state, name, id)
    tmp = Access.key(:tmp)
    state = put_in(state, [tmp, :tracking, name, id], cur + 1)
    put_in(state, [tmp, :records, name, id], record)
  end

  # defp rm(state, managed, record) do
  # end

  # do_manage
  @spec do_manage_path(State.t(), atom, :add | :rm, [record], keyword) ::
          State.t()
  defp do_manage_path(state, entity_name, action, records, path) do
    Enum.reduce(path, state, fn {path_entry, sub_path}, acc ->
      IO.inspect({path_entry, sub_path}, label: "do_manage_path path")
      %{children: children} = get_managed(acc.module, entity_name)
      spec = Map.fetch!(children, path_entry)

      do_manage_assoc(acc, entity_name, path_entry, spec, action, records, sub_path)
      # |> IO.inspect(label: "okayy")
    end)
  end

  # manage call:
  # do_manage_path(state, name, to_list.(orig), to_list.(new), normalize_preload(path))

  defp do_manage_assoc(
         state,
         entity_name,
         path_entry,
         {:one, assoc_name, fkey},
         :add,
         records,
         sub_path
       ) do
    assoc_managed = get_managed(state, assoc_name)

    IO.inspect({entity_name, path_entry, assoc_managed.name, fkey}, label: "ONE add")
    log(records, label: "records")

    {state, assoc_records} =
      Enum.reduce(records, {state, []}, fn record, {acc_state, acc_assoc_records} ->
        assoc_id = Map.fetch!(record, fkey)
        assoc = get_assoc_somehow(acc_state, path_entry, assoc_managed, assoc_id, record)
        {add(acc_state, assoc_managed, assoc), [assoc | acc_assoc_records]}
      end)

    do_manage_path(state, assoc_name, :add, assoc_records, sub_path)
  end

  defp do_manage_assoc(
         state,
         entity_name,
         path_entry,
         {:many, assoc_entity_name, fkey},
         :add,
         records,
         sub_path
       ) do
    %{id_key: id_key} = get_managed(state.module, entity_name)
    %{module: assoc_mod} = get_managed(state.module, assoc_entity_name)

    {assoc_records, ids} =
      Enum.reduce(records, {[], []}, fn record, {acc_assoc_records, acc_ids} ->
        case Map.fetch!(record, path_entry) do
          l when is_list(l) -> {l ++ acc_assoc_records, acc_ids}
          _ -> {acc_assoc_records, [id(id_key, record) | acc_ids]}
        end
      end)

    assoc_records =
      assoc_records ++
        state.repo.all(from(x in assoc_mod, where: field(x, ^fkey) in ^ids))

    Enum.each(assoc_records, &put(state, assoc_entity_name, &1))

    do_manage_path(state, assoc_entity_name, :add, assoc_records, sub_path)
  end

  @spec get_assoc_somehow(State.t(), atom, t, id, record) :: record
  defp get_assoc_somehow(state, path_entry, %{module: assoc_mod}, assoc_id, record) do
    assoc_from_record(record, path_entry) || state.repo.get(assoc_mod, assoc_id)
  end

  # Get the tracking (number of references) for the given entity and id.
  @spec tracking(map, atom, any) :: non_neg_integer
  defp tracking(%{tracking: tracking}, name, id),
    do: Map.fetch!(tracking, name)[id] || 0

  # Get the tmp tracking (number of references) for the given entity and id.
  @spec tracking_tmp(map, atom, any) :: non_neg_integer
  defp tracking_tmp(%{tmp: %{tracking: tracking}}, name, id) do
    Map.fetch!(tracking, name)[id] || 0
  end

  defp assoc_from_record(record, path_entry) do
    case record do
      %{^path_entry => %NotLoaded{}} -> nil
      %{^path_entry => %{} = assoc} -> assoc
      _ -> nil
    end
  end

  @doc """
  Add or remove a managed record, its tracked records, subscriptions, and
  tracking counters as needed according to `orig` being removed and `new` added.

  The `name` entity should be declared as `managed`.

  If `state` is a map, wrapping the managed state under a `:managed` key, it
  will be used as appropriate and returned re-wrapped.
  """
  @spec manage(state_or_wrapped, atom, [map] | map | nil, [map] | map | nil, path) ::
          state_or_wrapped
  def manage(state, name, orig, new, path \\ [])

  def manage(%{managed: managed_state} = state, name, orig, new, path) do
    %{state | managed: manage(managed_state, name, orig, new, path)}
  end

  def manage(state, name, orig, new, path) do
    log("MANAGE #{orig && orig.__struct__} -> #{new && new.__struct__}...")
    # %{children: children, module: module, setup: setup} = get_managed(mod, name)

    # new = if new && setup, do: setup.(new), else: new

    to_list = fn
      nil -> []
      i when is_map(i) -> [i]
      i -> i
    end

    do_manage_path(state, name, to_list.(orig), to_list.(new), normalize_preload(path))
  end

  @doc "Add a managed entity."
  defmacro managed(name, module, opts \\ []) do
    quote do
      @managed %Managed{
        children: Map.new(unquote(opts[:children] || [])),
        children_getters: Map.new(unquote(opts[:children_getters] || [])),
        fields: unquote(opts[:fields]),
        get_fn: unquote(opts[:get_fn]),
        id_key: unquote(opts[:id_key] || :id),
        module: unquote(module),
        name: unquote(name),
        prefilters: unquote(opts[:prefilters]),
        setup: unquote(opts[:setup]),
        subscribe: unquote(opts[:subscribe]),
        top: unquote(!!opts[:top]),
        unsubscribe: unquote(opts[:unsubscribe])
      }
    end
  end

  defmacro __before_compile__(%{module: mod}) do
    attr = &Module.get_attribute(mod, &1)

    validate_before_compile!(mod, attr.(:managed_repo), attr.(:managed))

    quote do
      @doc "Returns a list of all managed entity names."
      @spec __managed__ :: [atom]
      def __managed__, do: Enum.map(@managed, & &1.name)

      @doc "Returns the `t:Managed.t/0` for an entity by its name or module."
      @spec __managed__(atom) :: Managed.t() | nil
      def __managed__(name), do: Enum.find(@managed, &(&1.name == name or &1.module == name))

      @doc "Returns a list of managed entity names which are tracked."
      @spec __tracked__ :: [atom]
      @tracked tracked_entities(@managed)
      def __tracked__, do: @tracked

      @doc """
      Given a managed entity name or module and a field, return the preload
      function which will take a record and state and return the association
      record or list of records.
      """
      @spec __preload_fn__(atom, atom, module) :: (map, State.t() -> map | [map]) | nil
      def __preload_fn__(name, key, repo) do
        case Enum.find(@managed, &(&1.name == name or &1.module == name)) do
          %{children: %{^key => preload_spec}} -> preload_fn(preload_spec, repo)
          %{} = managed -> preload_fn({:repo, key, managed}, repo)
          nil -> nil
        end
      end
    end
  end

  @doc """
  Given the list of `%Managed{}`s, return the names of those referred to with
  `:one` relationships.
  """
  @spec tracked_entities([t]) :: [atom]
  def tracked_entities(manageds) do
    manageds
    |> Enum.reduce([], fn %{children: children}, acc ->
      children
      |> Enum.reduce([], fn
        {_key, {:one, entity, _fkey}}, acc2 -> [entity | acc2]
        _, acc2 -> acc2
      end)
      |> Kernel.++(acc)
    end)
    |> Enum.uniq()
  end

  @spec validate_before_compile!(module, module, list) :: :ok
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def validate_before_compile!(mod, repo, managed) do
    for %{children: children, module: module, name: name, subscribe: sub, unsubscribe: unsub} <-
          managed do
      inf = "in #{inspect(mod)} for #{name}"

      if (sub != nil and is_nil(unsub)) or (unsub != nil and is_nil(sub)),
        do: raise("Must have both :subscribe and :unsubscribe or neither #{inf}.")

      for {key, preload_spec} <- children do
        related =
          case module.__schema__(:association, key) do
            %{related: r} -> r
            nil -> raise "Expected association #{key} on #{inspect(module)}."
          end

        unless Enum.find(managed, &(&1.module == related)),
          do: raise("#{inspect(related)} must be tracked #{inf}.")

        function_exported?(module, :__schema__, 1) ||
          raise "Schema module expected: #{inspect(module)} #{inf}"

        preload_fn(preload_spec, repo) ||
          raise "Invalid preload spec: #{inspect(preload_spec)} #{inf}"
      end
    end

    :ok
  end

  @spec get(state_or_wrapped, atom, id) :: any
  def get(%{managed: managed_state}, name, id) do
    get(managed_state, name, id)
  end

  def get(%{index: index}, name, id) do
    Indexed.get(index, name, id)
  end

  @spec put(state_or_wrapped, atom, record) :: :ok
  def put(%{managed: managed_state}, name, record) do
    put(managed_state, name, record)
  end

  def put(%{index: index}, name, record) do
    Indexed.put(index, name, record)
  end

  @spec drop(state_or_wrapped, atom, id) :: :ok | :error
  def drop(%{managed: managed_state}, name, id) do
    drop(managed_state, name, id)
  end

  def drop(%{index: index}, name, id) do
    Indexed.drop(index, name, id)
  end

  @spec get_index(state_or_wrapped, atom, Indexed.prefilter()) :: list | map | nil
  def get_index(state, name, prefilter \\ nil, order_hint \\ nil)

  def get_index(%{managed: managed_state}, name, prefilter, order_hint) do
    get_index(managed_state, name, prefilter, order_hint)
  end

  def get_index(%{index: index}, name, prefilter, order_hint) do
    Indexed.get_index(index, name, prefilter, order_hint)
  end

  @spec get_records(state_or_wrapped, atom, Indexed.prefilter(), order_hint | nil) :: [record] | nil
  def get_records(state, name, prefilter, order_hint \\ nil)

  def get_records(%{managed: managed_state}, name, prefilter, order_hint) do
    get_records(managed_state, name, prefilter, order_hint)
  end

  def get_records(%{index: index}, name, prefilter, order_hint) do
    Indexed.get_records(index, name, prefilter, order_hint)
  end

  defp maybe_subscribe(mod, name, id) do
    with %{subscribe: sub} when is_function(sub) <- get_managed(mod, name),
         do: sub.(id) |> IO.inspect(label: "SUBSCRIBED #{name} #{id}")
  end

  defp maybe_unsubscribe(mod, name, id) do
    with %{unsubscribe: usub} when is_function(usub) <- get_managed(mod, name),
         do: usub.(id) |> IO.inspect(label: "UNSUBSCRIBED #{name} #{id}")
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

  # Get the %Managed{} or raise a nice error.
  @spec get_managed(State.t() | module, atom) :: Managed.t()
  defp get_managed(%{module: mod}, name), do: get_managed(mod, name)

  defp get_managed(mod, name) do
    mod.__managed__(name) ||
      raise ":#{name} must have a managed declaration on #{inspect(mod)}."
  end

  # Get the indexing "id" of a particular managed record.
  @spec id(t | id_key | nil, map) :: any
  # defp id(%{id_key: id_key}, record), do: id(id_key, record)
  defp id(id_key, record) when is_function(id_key), do: id_key.(record)
  defp id(nil, record), do: raise("No id_key found for #{inspect(record)}")
  defp id(id_key, record), do: Map.get(record, id_key)

  @doc """
  Given a preload function spec, create a preload function. `key` is the key of
  the parent entity which should be filled with the child or list of children.

  See `t:preload/0`.
  """
  @spec preload_fn(preload_spec, module) :: (map, State.t() -> any) | nil
  def preload_fn({:one, name, key}, _repo) do
    fn record, state ->
      Indexed.get(state.index, name, Map.get(record, key))
    end
  end

  def preload_fn({:many, name, pf_key}, repo) do
    preload_fn({:many, name, pf_key, nil}, repo)
  end

  def preload_fn({:many, name, pf_key, order_hint}, _repo) do
    fn record, state ->
      pf = if pf_key, do: {pf_key, record.id}, else: nil
      Indexed.get_records(state.index, name, pf, order_hint) || []
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
           do: repo.get(related, id) |> IO.inspect(label: "REPO GOT")
    end
  end

  def preload_fn(_, _), do: nil

  @doc "Preload associations recursively."
  @spec preload(map | [map] | nil, State.t(), atom | list) :: [map] | map | nil
  def preload(record_or_list, state, preload) do
    resolve(record_or_list, state, preload: preload)
  end

  @doc """
  Prepare some data.

  ## Options

  * `:preload` - Which data to preload. eg. `[:roles, markets: :locations]`
  """
  @spec resolve(map | [map] | nil, State.t(), keyword | map) :: [map] | map | nil
  def resolve(record_or_list, state, opts \\ [])
  def resolve(nil, _, _), do: nil

  def resolve(record_or_list, state, opts) when is_list(record_or_list) do
    Enum.map(record_or_list, &resolve(&1, state, opts))
  end

  def resolve(record_or_list, %{module: mod} = state, opts) do
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

    Enum.reduce(listify.(opts[:preload]), record, fn
      {key, sub_pl}, acc ->
        preloaded = acc |> preload.(key) |> resolve(state, preload: listify.(sub_pl))
        Map.put(acc, key, preloaded)

      key, acc ->
        Map.put(acc, key, preload.(acc, key))
    end)
  end

  @doc "Wrap a value (or nil) in an :ok or :error tuple."
  @spec resolve_return(any) :: {:ok, any} | {:error, :not_found}
  def resolve_return(nil), do: {:error, :not_found}
  def resolve_return(val), do: {:ok, val}

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
