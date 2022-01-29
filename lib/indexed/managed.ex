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
          children: [passengers: {:get_records, :people, nil, :name}]

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
  alias Ecto.Association.{BelongsTo, Has, NotLoaded}
  alias __MODULE__

  defmodule State do
    @moduledoc "A piece of GenServer state for Managed."
    defstruct [:index, :loader, :module, :repo, :source, :tracking]

    @typedoc """
    * `:loader` - Ephemeral dataloader, reset to `nil` after an operation.
    """
    @type t :: %__MODULE__{
            index: Indexed.t(),
            loader: Dataloader.t() | nil,
            module: module,
            repo: module,
            source: Dataloader.Ecto.t(),
            tracking: Indexed.Managed.tracking()
          }
  end

  @typedoc """
  A preload spec which is used to build the preload function. This function
  receives the record and the state and must return the preloaded value.
  (Note that the record is only used for reference -- it is not returned.)

  * `{:get, entity_name, id_key}` - Preload function should get a record of
    `entity_name` with id matching the id found under `id_key` of the record.
  * `{:get_records, entity_name, pf_key, order_hint}` - Preload function should
    use `Indexed.get_records/4`. If `pf_key` is not null, it will be replaced
    with `{pfkey, id}` where `id` is the record's id.
  * `{:repo, key, managed}` - Preload function should use `Repo.get/2` with the
    assoc's module and the id in the foreign key field for `key` in the record.
    This is the default when a child/preload_spec isn't defined for an assoc.
  """
  @type preload_spec ::
          {:get, entity_name :: atom, id_key :: atom}
          | {:get_records, entity_name :: atom, pf_key :: atom | nil, Indexed.order_hint()}
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
  @type tracking_status :: %{Ecto.UUID.t() => non_neg_integer}

  @typep data_opt :: Indexed.Actions.Warm.data_opt()

  # Path to follow when warming or updating data. Uses same format as preload.
  @typep path :: atom | list

  defstruct [
    :children,
    :children_getters,
    :id_key,
    :get_fn,
    :module,
    :name,
    :setup,
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
  * `:unsubscribe` - 1-arity function which unsubscribes to changes by id.
  """
  @type t :: %Managed{
          children: %{atom => preload_spec},
          children_getters: %{atom => {module, atom}},
          id_key: atom | (map -> any),
          get_fn: (any -> map) | nil,
          module: module,
          name: atom,
          setup: (map -> map) | nil,
          subscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil,
          unsubscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil
        }

  defmacro __using__(repo: repo) do
    quote do
      import unquote(__MODULE__)
      @before_compile unquote(__MODULE__)
      @managed_repo unquote(repo)
      Module.register_attribute(__MODULE__, :managed, accumulate: true)

      @doc "Returns a freshly initialized state for `Indexed.Managed`."
      @spec warm(atom, data_opt, path) :: State.t()
      def warm(entity_name, data, path) do
        state = init_state(__MODULE__, unquote(repo))
        warm(state, entity_name, data, path)
      end
    end
  end

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init_state(module, module) :: State.t()
  def init_state(mod, repo) do
    source = Dataloader.Ecto.new(repo)
    loader = new_loader(source)
    tracking = Enum.reduce(mod.__tracked__(), %{}, &Map.put(&2, &1, %{}))
    %State{loader: loader, module: mod, source: source, tracking: tracking}
  end

  @doc "Loads data into index, populating `:tracked` and subscribing as needed."
  @spec warm(State.t(), atom, data_opt, path) :: State.t()
  def warm(state, entity_name, data, path) do
    # Entities for which :data is defined will be our branching-off points.
    start_entities =
      Enum.reduce(warm_args, [], fn {k, opts}, acc ->
        if Keyword.get(opts, :data), do: [k | acc], else: acc
      end)

    source = Dataloader.Ecto.new(repo)
    loader = new_loader(source)
    tracking = Enum.reduce(mod.__tracked__(), %{}, &Map.put(&2, &1, %{}))
    state = %State{loader: loader, module: mod, source: source, tracking: tracking}
    warm_args = Enum.reduce(start_entities, {warm_args, state}, &do_warm/2)

    %{state | index: Indexed.warm(warm_args)}
  end

  defp do_warm(entity, {warm_args, state}) do
    %{children: children} = get_managed(mod, name)
    # %{children: children, setup: setup} = get_managed(mod, name)

    Enum.reduce(children, {warm_args, state}, fn {key, _preload_spec}, {warm_args, state} ->
      assoc = module.__schema__(:association, key)
      do_manage_child(acc, assoc, orig, new)
    end)
  end

  @doc "Add a managed entity."
  defmacro managed(name, module, opts \\ []) do
    quote do
      @managed %Managed{
        children: Map.new(unquote(opts[:children] || [])),
        children_getters: Map.new(unquote(opts[:children_getters] || [])),
        get_fn: unquote(opts[:get_fn]),
        id_key: unquote(opts[:id_key] || :id),
        module: unquote(module),
        name: unquote(name),
        setup: unquote(opts[:setup]),
        subscribe: unquote(opts[:subscribe]),
        unsubscribe: unquote(opts[:unsubscribe])
      }
    end
  end

  defmacro __before_compile__(%{module: mod}) do
    attr = &Module.get_attribute(mod, &1)
    source = Dataloader.Ecto.new(attr.(:managed_repo))

    validate_before_compile!(mod, source, attr.(:managed))

    quote do
      @doc "Returns a list of all managed entity names."
      @spec __managed__ :: [atom]
      def __managed__, do: Enum.map(@managed, & &1.name)

      @doc "Returns the `t:Managed.t/0` for an entity by its name or module."
      @spec __managed__(atom) :: Managed.t() | nil
      def __managed__(name), do: Enum.find(@managed, &(&1.name == name or &1.module == name))

      @doc "Returns a list of managed entity names which are tracked."
      @spec __tracked__ :: [atom]
      @tracked @managed |> Enum.filter(&tracked?/1) |> Enum.map(& &1.name)
      def __tracked__, do: @tracked

      @doc "Returns the `t:Managed.t/0` for `name` if it is a tracked entity."
      @spec __tracked__(atom) :: Managed.t() | nil
      def __tracked__(name) do
        if name in @tracked, do: __managed__(name), else: nil
      end

      @doc """
      Given a managed entity name or module and a field, return the preload
      function which will take a record and state and return the association
      record or list of records.
      """
      @spec __preload_fn__(atom, atom, Dataloader.t()) :: (map, State.t() -> map | [map]) | nil
      def __preload_fn__(name, key, source) do
        case Enum.find(@managed, &(&1.name == name or &1.module == name)) do
          %{children: %{^key => preload_spec}} -> preload_fn(__MODULE__, preload_spec)
          %{} = managed -> preload_fn({:dataloader, key, managed}, source)
          nil -> nil
        end
      end
    end
  end

  @spec validate_before_compile!(module, Dataloader.Ecto.t(), list) :: :ok
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def validate_before_compile!(mod, source, managed) do
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

        preload_fn(preload_spec, source) ||
          raise "Invalid preload spec: #{inspect(preload_spec)} #{inf}"
      end
    end

    :ok
  end

  @doc """
  Add or remove a managed record, its tracked records, subscriptions, and
  tracking counters as needed according to `orig` being removed and `new` added.

  The `name` entity should be declared as `managed`.
  """
  @spec manage(State.t(), atom, map | nil, map | nil) :: State.t()
  def manage(%{module: mod} = state, name, orig, new) do
    log("MANAGE #{orig && orig.__struct__} -> #{new && new.__struct__}...")
    %{children: children, module: module, setup: setup} = get_managed(mod, name)

    new = if new && setup, do: setup.(new), else: new

    state =
      Enum.reduce(children, state, fn {key, _preload_spec}, acc ->
        log("key: #{key}")
        assoc = module.__schema__(:association, key)
        do_manage_child(acc, assoc, orig, new)
      end)

    if new do
      log({orig, new}, label: "wordmate")
      Indexed.put(state.index, name, drop_associations(new))
    else
      log({orig, new}, label: "wtfmate")
      Indexed.drop(state.index, name, id(mod, name, orig))
    end

    # if new,
    #   do: Server.put(state, name, DrugMule.drop_associations(new)),
    #   else: Server.drop(state, name, id(mod, name, orig))

    state
  end

  defp log(val, opts \\ []) do
    if Process.get(:bb) do
      # if Process.get(:bla) do
      str = if is_binary(val), do: val, else: inspect(val)

      case opts[:label] do
        nil -> IO.puts(str)
        lbl -> IO.puts("#{lbl}: #{str}")
      end
    end

    val
  end

  @spec do_manage_child(State.t(), Ecto.Association.t(), map | nil, map | nil) :: State.t()
  defp do_manage_child(%{module: mod} = state, %BelongsTo{cardinality: :one} = assoc, orig, new) do
    %{field: field, owner_key: owner_key, related: related} =
      assoc
      |> log(label: "SHIT")

    %{get_fn: get_fn, module: module, name: name} = assoc_managed = get_managed(mod, related)

    log(orig, label: "oooooOOORIG #{field} - #{owner_key}")
    log(new, label: "oooooNNNEW #{field} - #{owner_key}")

    assoc_orig =
      with %NotLoaded{} <- orig && Map.get(orig, field) do
        orig
        |> Managed.preload(state, field)
        |> IO.inspect(label: "pppppp")
        |> Map.get(field)
        |> log(label: "LLLOADED ORIG")
      end

    # assoc_new = new && Map.get(new, field)
    assoc_new =
      with %NotLoaded{} <- new && Map.get(new, field) do
        IO.inspect({get_fn, new, Map.get(new, owner_key)}, label: "CHECKCHECK")

        case get_fn && new && Map.get(new, owner_key) do
          nil -> nil
          id -> get_fn.(id)
        end
        |> log(label: "LLLOADED NEW")
      end

    log(assoc_orig, label: "ASSOC ORIG")
    log(assoc_new, label: "ASSOC NEW")

    track = &do_track(&1, &2, name, assoc, orig, new)
    mod_or_nil? = fn it, m -> match?(%^m{}, it) or is_nil(it) end

    if mod_or_nil?.(assoc_orig, module) and mod_or_nil?.(assoc_new, module) and
         not (is_nil(assoc_orig) and is_nil(assoc_new)) do
      log(label: "WORD1")
      # Assoc in orig/new was preloaded. Manage recursively.
      state = if tracked?(assoc_managed), do: track.(state, false), else: state
      manage(state, name, assoc_orig, assoc_new)
    else
      log(label: "WORD2")
      if tracked?(assoc_managed), do: track.(state, true), else: state
    end
  end

  defp do_manage_child(%{module: mod} = state, %Has{cardinality: :many} = assoc, orig, new) do
    %{field: field, related: related, related_key: related_key} = assoc
    %{name: related_name, module: related_module} = get_managed(mod, related)

    log("ok: #{inspect({field, related_name})}")

    assoc_orig =
      with %NotLoaded{} <- orig && Map.get(orig, field) do
        orig
        |> Managed.preload(state, field)
        |> Map.get(field)
        |> log(label: "LOADED ORIG")
      end
      |> log(label: "ORIG")

    # If the assoc isn't loaded in the new record, load it.
    # First try a custom function defined in :children_getters.
    # If none, use a generic fetch-by-foreign-key approach.
    assoc_new =
      with %NotLoaded{} <- new && Map.get(new, field) do
        case get_in(get_managed(mod, new.__struct__), [
               Access.key(:children_getters),
               related_name
             ]) do
          {fn_mod, fn_name} ->
            apply(fn_mod, fn_name, [new.id])

          nil ->
            new_id = new.id
            # TODOO
            state.repo.all(from r in related_module, where: field(r, ^related_key) == ^new_id)
        end
      end

    do_manage_has_many(state, related_name, assoc_orig, assoc_new)
  end

  # Update in-state tracking and maybe
  @spec do_track(State.t(), boolean, atom, Ecto.Association.t(), map | nil, map | nil) ::
          State.t()
  defp do_track(state, fetch_record?, name, %{owner_key: owner_key} = assoc, orig, new) do
    orig_id = orig && Map.get(orig, owner_key)
    new_id = new && Map.get(new, owner_key)
    state = state |> add_tracked(name, new_id) |> rm_tracked(name, orig_id)
    if fetch_record?, do: do_tracked_record(state, name, assoc, orig, new), else: state
  end

  # Add or remove the tracked record in the cache, based on a change to its
  # parent, `orig` and `new`.

  # `orig` or `new` may be `nil` to indicate that the record has been created or
  # deleted. If no change is needed, none is made. `state` (tracking maps) must
  # already reflect the change being made, so `update_tracked_for/4` or similar
  # must be called first.

  # For the first instance of the association, it will be loaded via `:get_fn`.
  @spec do_tracked_record(State.t(), atom, Ecto.Association.t(), map | nil, map | nil) ::
          State.t()
  defp do_tracked_record(%{module: mod, tracking: tracking} = state, name, assoc, orig, new) do
    %{field: field, owner_key: owner_key, related: related} = assoc
    orig_tracked_id = orig && Map.get(orig, owner_key)
    new_tracked_id = new && Map.get(new, owner_key)
    tracking_map = Map.fetch!(tracking, name)
    tracking_count = tracking_map[new_tracked_id]

    state =
      if new_tracked_id && tracking_count == 1 do
        r1 = orig && Map.get(orig, field)
        r2 = new && Map.get(new, field)
        record = do_get_record(r1, r2, related, mod, name, new_tracked_id)
        manage(state, name, r1, record)
      else
        state
      end

    if orig_tracked_id && is_nil(tracking_count),
      do: Indexed.drop(state.index, name, orig_tracked_id)

    state
  end

  # Load an associated record.
  #
  # - First see if `new` is an already-loaded instance.
  # - If not, use the `get_fn` on the managed configuration.
  # - If that function isn't defined, raise -- it should be!
  @spec do_get_record(any, any, module, module, atom, any) :: map
  defp do_get_record(_, %related{} = new, related, _, _, _) do
    new
  end

  defp do_get_record(_, _, _, mod, name, new_tracked_id) do
    %{get_fn: get_fn} = mod.__tracked__(name)
    get_fn || raise ":get_fn not defined on #{name} managed declaration."
    get_fn.(new_tracked_id)
  end

  # For a managed record's managed children, call manage on each as needed.
  @spec do_manage_has_many(State.t(), atom, [map] | nil, [map] | nil) :: State.t()
  defp do_manage_has_many(state, entity_name, orig_list, new_list)
       when is_list(orig_list) and is_list(new_list) do
    # Manage the record if it's remained in the list or if it was removed.
    state =
      Enum.reduce(orig_list, state, fn orig_record, acc ->
        new_record = Enum.find(new_list, &(&1.id == orig_record.id))

        if new_record,
          do: manage(acc, entity_name, orig_record, new_record),
          else: manage(acc, entity_name, orig_record, nil)
      end)

    # Manage the record if it's been added to the list.
    Enum.reduce(new_list, state, fn new_record, acc ->
      if Enum.any?(orig_list, &(&1.id == new_record.id)),
        do: acc,
        else: manage(acc, entity_name, nil, new_record)
    end)
  end

  defp do_manage_has_many(state, entity_name, orig_list, nil) when is_list(orig_list) do
    Enum.reduce(orig_list, state, &manage(&2, entity_name, &1, nil))
  end

  defp do_manage_has_many(state, entity_name, nil, new_list) when is_list(new_list) do
    Enum.reduce(new_list, state, &manage(&2, entity_name, nil, &1))
  end

  defp do_manage_has_many(state, _, _, _), do: state

  @doc """
  For each tracked child of the entity in `orig` and `new`, update the tracking.

  This is useful when warming the `Indexed` cache when you'd prefer to fetch all
  the records at once and then iterate through to update the `t:tracking/0`.
  """
  @spec update_tracked_for(State.t(), atom, map | nil, map | nil) :: State.t()
  def update_tracked_for(%{module: mod} = state, name, orig, new) do
    %{children: children, module: module} = get_managed(mod, name)

    Enum.reduce(children, state, fn {field, _preload_spec}, acc ->
      %{owner_key: owner_key, related: related} = module.__schema__(:association, field)
      managed = get_managed(mod, related)

      if tracked?(managed) do
        orig_id = orig && Map.get(orig, owner_key)
        new_id = new && Map.get(new, owner_key)
        acc |> add_tracked(managed.name, new_id) |> rm_tracked(managed.name, orig_id)
      else
        acc
      end
    end)
  end

  @doc "Add a new id to `name` tracking. Subscribe if it's new."
  @spec add_tracked(State.t(), atom, Ecto.UUID.t() | nil) :: State.t()
  def add_tracked(state, _name, nil), do: state

  def add_tracked(%{module: mod, tracking: tracking} = state, name, id) do
    with {:a, %{} = this_tracking} <- {:a, tracking[name]},
         {:n, {:ok, num}} <- {:n, Map.fetch(this_tracking, id)} do
      put_in(state, [Access.key(:tracking), name, id], num + 1)
    else
      {:a, bad} ->
        raise "Expected map in tracking[#{inspect(name)}], got #{inspect(bad)}"

      {:n, :error} ->
        with %{subscribe: sub} when is_function(sub) <- mod.__tracked__(name), do: sub.(id)
        put_in(state, [Access.key(:tracking), name, id], 1)
    end
  end

  @doc "Remove an id from `name` tracking. Unsubscribe if it was the last one."
  @spec rm_tracked(State.t(), atom, Ecto.UUID.t() | nil) :: State.t()
  def rm_tracked(state, _name, nil), do: state

  def rm_tracked(%{module: mod, tracking: tracking} = state, name, id) do
    this_tracking = Map.fetch!(tracking, name)

    case Map.get(this_tracking, id) do
      1 ->
        with %{unsubscribe: usub} when is_function(usub) <- mod.__tracked__(name), do: usub.(id)
        %{state | tracking: Map.put(tracking, name, Map.delete(this_tracking, id))}

      n when n > 1 ->
        log(n, label: "ennn")
        log(this_tracking, label: "thisss (#{id})")
        %{state | tracking: Map.put(tracking, name, Map.put(this_tracking, id, n - 1))}
    end
  end

  # Get the %Managed{} or raise a nice error.
  @spec get_managed(module, atom) :: Managed.t()
  defp get_managed(mod, name) do
    mod.__managed__(name) ||
      raise ":#{name} must have a managed declaration on #{inspect(mod)}."
  end

  # Get the indexing "id" of a particular managed record.
  @spec id(module, atom, map) :: any
  defp id(mod, name, record) do
    case mod.__managed__(name) do
      %{id_key: id_key} when is_function(id_key) -> id_key.(record)
      %{id_key: id_key} -> Map.get(record, id_key)
      nil -> raise ":#{name} must have a managed declaration on #{inspect(mod)}."
    end
  end

  @doc """
  Given a `t:Managed.t/0`, return true if it is tracked.

  Being tracked means that `:get_fn` is defined.
  """
  @spec tracked?(Managed.t()) :: boolean
  def tracked?(%{get_fn: fun}) when is_function(fun), do: true
  def tracked?(_), do: false

  @doc """
  Given a preload function spec, create a preload function. `key` is the key of
  the parent entity which should be filled with the child or list of children.

  See `t:preload/0`.
  """
  @spec preload_fn(preload_spec, module) :: (map, State.t() -> any) | nil
  def preload_fn({:get, name, key}, _source) do
    fn record, state ->
      Indexed.get(state.index, name, Map.get(record, key))
    end
  end

  def preload_fn({:get_records, name, pf_key, order_hint}, _source) do
    fn record, state ->
      pf = if pf_key, do: {pf_key, record.id}, else: nil
      Indexed.get_records(state.index, name, pf, order_hint) || []
    end
  end

  def preload_fn({:dataloader, key, %{module: module}}, source) do
    {owner_key, related} =
      case module.__schema__(:association, key) do
        %{owner_key: k, related: r} -> {k, r}
        nil -> raise "Expected association #{key} on #{inspect(module)}."
      end

    fn record, _state ->
      with id when id != nil <- Map.get(record, owner_key) do
        source
        |> new_loader()
        |> Dataloader.load(:db, related, id)
        |> Dataloader.run()
        |> Dataloader.get(:db, related, id)
      end
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
          mod.__preload_fn__(record_mod, key, state.source) ||
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

  @spec new_loader(Dataloader.Source.t()) :: Dataloader.t()
  defp new_loader(source) do
    Dataloader.add_source(Dataloader.new(), :db, source)
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
