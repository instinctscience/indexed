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
  alias Ecto.Association.{BelongsTo, Has, NotLoaded}
  alias Indexed.Actions.Warm
  alias __MODULE__

  defmodule State do
    @moduledoc "A piece of GenServer state for Managed."
    defstruct [:index, :module, :repo, :tracking]

    @typedoc """
    # * `:loader` - Ephemeral dataloader, reset to `nil` after an operation.
    """
    @type t :: %__MODULE__{
            index: Indexed.t() | nil,
            module: module,
            repo: module,
            tracking: Indexed.Managed.tracking()
          }
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
          | {:many, entity_name :: atom, pf_key :: atom | nil, Indexed.order_hint()}
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

  @type data_opt :: Warm.data_opt()

  # Path to follow when warming or updating data. Uses same format as preload.
  @type path :: atom | list

  @typep id_key :: atom | (map -> any)

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
          fields: [atom | Indexed.Entity.field()],
          id_key: id_key,
          get_fn: (any -> map) | nil,
          module: module,
          name: atom,
          prefilters: [atom | keyword] | nil,
          setup: (map -> map) | nil,
          subscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil,
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
        state = init_state(__MODULE__, unquote(repo))
        warm(state, entity_name, data_opt, path)
      end
    end
  end

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init_state(module, module) :: State.t()
  def init_state(mod, repo) do
    tracking = Enum.reduce(mod.__tracked__(), %{}, &Map.put(&2, &1, %{}))
    %State{module: mod, repo: repo, tracking: tracking}
  end

  @doc "Loads data into index, populating `:tracked` and subscribing as needed."
  @spec warm(State.t(), atom, data_opt, path) :: State.t()
  def warm(%{module: mod} = state, entity_name, data_opt, path) do
    warm_args =
      Enum.reduce(mod.__managed__(), [], fn entity, acc ->
        data = if entity == entity_name, do: data_opt, else: []
        managed = get_managed(mod, entity)

        Keyword.put(acc, entity,
          data: data,
          fields: managed.fields,
          prefilters: managed.prefilters
        )
      end)

    managed = get_managed(mod, entity_name)
    {_, _, records} = Warm.resolve_data_opt(data_opt, entity_name, managed.fields)

    state = %{state | index: Indexed.warm(warm_args)}

    do_manage_path(entity_name, [], records, normalize_preload(path), state, true)
  end

  # do_manage
  @spec do_manage_path(atom, [Indexed.record()], [Indexed.record()], keyword, State.t(), boolean) ::
          State.t()
  defp do_manage_path(entity_name, orig_records, new_records, path, state, already_loaded \\ false) do
    managed = get_managed(state, entity_name)
    state = Enum.reduce(records, state, &add_tracked(&2, entity_name, id(managed, &1)))

    # TODO add records and tracking with shared code.
    Enum.reduce(path, state, fn {path_entry, sub_path}, acc ->
      IO.inspect({path_entry, sub_path}, label: "do_manage_path path")
      %{children: children} = get_managed(acc.module, entity_name)
      spec = Map.fetch!(children, path_entry)

      do_manage_assoc(entity_name, path_entry, spec, orig_records, new_records, sub_path, acc)
      # |> IO.inspect(label: "okayy")
    end)
  end

  # manage call:
  # do_manage_path(name, to_list.(orig), to_list.(new), normalize_preload(path), state)

  defp do_manage_assoc(
         entity_name,
         path_entry,
         {:one, assoc_name, fkey},
         orig_records,
         new_records,
         sub_path,
         state
       ) do
    # ONE: {:comments, :users, :author_id}
    log({entity_name, assoc_name, fkey}, label: "ONE")
    log(orig_records, label: "orig_records")
    log(new_records, label: "new_records")

    %{id_key: id_key} = get_managed(state, entity_name)
    %{module: assoc_mod} = assoc_managed = get_managed(state, assoc_name)

    log([assoc_mod: assoc_mod, assoc_name: assoc_name], label: "...")

    orig_assoc_ids = Enum.map(orig_records, &Map.fetch!(&1, fkey))
    new_assoc_ids = Enum.map(new_records, &Map.fetch!(&1, fkey))
    log({orig_assoc_ids, new_assoc_ids}, label: "orig-new assoc ids")

    # TODO maybe remove record from list for speed? share info another-how?
    {state, new_assoc_records} =
      Enum.reduce(new_records, {state, []}, fn new, {acc_state, acc_nars} ->
        new_id = id(id_key, new)
        new_assoc_id = Map.fetch!(new, fkey)

        add = fn ->
          {state, bnrs} = do_add_assoc(path_entry, new_assoc_id, assoc_managed, new, acc_state)
          {state, bnrs ++ acc_nars}
        end

        case Enum.find(orig_records, &(id(id_key, &1) == new_id)) do
          # Record is present in both lists and the assoc id remains.
          %{^fkey => ^new_assoc_id} -> {acc_state, acc_nars}
          # Record is present in both lists but assoc id changed.
          %{} -> add.()
          # Record is present only in the new list.
          nil -> add.()
        end
      end)

    {state, gone_assoc_records} =
      Enum.reduce(orig_records, {state, []}, fn orig, {acc_state, acc_gars} ->
        orig_id = id(id_key, orig)
        orig_assoc_id = Map.fetch!(orig, fkey)

        rm = fn ->
          {state, lrrs} = do_rm_assoc(assoc_name, orig_assoc_id, acc_state)
          {state, lrrs ++ acc_gars}
        end

        case Enum.find(new_records, &(id(id_key, &1) == orig_id)) do
          # Record is present in both lists and the assoc id remains.
          %{^fkey => ^orig_assoc_id} -> {acc_state, acc_gars}
          # Record is present in both lists but assoc id changed.
          %{} -> rm.()
          # Record is present only in the old list.
          nil -> rm.()
        end
      end)

    state = do_manage_path(assoc_name, gone_assoc_records, new_assoc_records, sub_path, state)

    # # state = Enum.reduce(ids, state, &add_tracked(&2, assoc_name, &1))

    # already_ids = get_index(state, assoc_entity_name) || []
    # log(already_ids, label: "already_ids")

    # ids =
    #   Enum.reject(new_ids, &(&1 in already_ids))
    #   |> log(label: "but these")

    # assoc_records =
    #   state.repo.all(from i in assoc_mod, where: field(i, ^id_key) in ^ids)
    #   |> log(label: "assoc records")

    # Enum.each(assoc_records, &Indexed.put(state.index, assoc_name, &1))

    log(state, label: "state")
    if Process.get(:bb), do: raise("done")
    state
    # do_manage_path(assoc_entity_name, [], assoc_records, sub_path, state)
  end

  defp do_manage_assoc(
         entity_name,
         path_entry,
         {:many, assoc_entity_name, fkey},
         orig_records,
         new_records,
         sub_path,
         state
       ) do
    %{id_key: id_key} = get_managed(state.module, entity_name)

    %{id_key: assoc_id_key, module: assoc_mod, name: assoc_name} =
      assoc_managed = get_managed(state.module, assoc_entity_name)

    idx = get_index(state, entity_name) || []
    already_ids = Enum.reduce(new_records, [], &if(&1.id in idx, do: &2, else: [&1.id | &2]))

    ids =
      Enum.reduce(new_records, [], fn record, acc ->
        id = id(id_key, record)
        if id in already_ids, do: acc, else: [id | acc]
      end)

    assoc_records = state.repo.all(from(i in assoc_mod, where: field(i, ^fkey) in ^ids))

    Enum.each(assoc_records, &Indexed.put(state.index, assoc_name, &1))

    fun = &add_tracked(&2, assoc_entity_name, id(assoc_id_key, &1))
    state = Enum.reduce(assoc_records, state, fun)

    do_manage_path(assoc_entity_name, [], assoc_records, sub_path, state)
  end

  defp do_add_assoc(path_entry, assoc_id, assoc_managed, record, state) do
    %{module: assoc_mod, name: assoc_name} = assoc_managed
    state = add_tracked(state, assoc_name, assoc_id)

    if 1 == tracking(assoc_name, assoc_id, state) do
      %{} =
        record =
        assoc_from_record(record, path_entry) ||
          get(state, assoc_name, assoc_id) ||
          state.repo.get(assoc_mod, assoc_id)

      put(state, assoc_name, drop_associations(record))
      {state, [record]}
    else
      {state, []}
    end
  end

  defp do_rm_assoc(assoc_name, assoc_id, state) do
    state = rm_tracked(state, assoc_name, assoc_id)

    if 0 == tracking(assoc_name, assoc_id, state) do
      record = get(state, assoc_name, assoc_id)
      drop(state, assoc_name, assoc_id)
      {state, [record]}
    else
      {state, []}
    end
  end

  # Get the tracking (number of references) for the given entity and id.
  @spec tracking(atom, any, State.t()) :: non_neg_integer
  defp tracking(name, id, %{tracking: tracking}) do
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

    do_manage_path(name, to_list.(orig), to_list.(new), normalize_preload(path), state)

    # state =
    #   path
    #   |> normalize_preload()
    #   |> Enum.reduce(state, fn {key, _preload_spec}, acc ->
    #     log("key: #{key}")
    #     assoc = module.__schema__(:association, key)
    #     # do_manage_assoc(acc, assoc, orig, new)
    #   end)

    # if new do
    #   log({orig, new}, label: "wordmate")
    #   Indexed.put(state.index, name, drop_associations(new))
    # else
    #   log({orig, new}, label: "wtfmate")
    #   Indexed.drop(state.index, name, id(mod, name, orig))
    # end

    # if new,
    #   do: Server.put(state, name, DrugMule.drop_associations(new)),
    #   else: Server.drop(state, name, id(mod, name, orig))

    # state
  end

  # defp do_manage_path(entity_name, orig_records, new_records, path, state) do
  #   Enum.reduce(path, state, fn {path_entry, sub_path}, acc ->
  #     %{children: children} = get_managed(acc.module, entity_name)
  #     spec = Map.fetch!(children, path_entry)

  #     do_manage_assoc(entity_name, path_entry, orig_records, new_records, spec, sub_path, acc)
  #   end)
  # end

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

  @spec get(state_or_wrapped, atom, Indexed.id()) :: any
  def get(%{managed: managed_state}, name, id) do
    get(managed_state, name, id)
  end

  def get(%{index: index}, name, id) do
    Indexed.get(index, name, id)
  end

  @spec put(state_or_wrapped, atom, Indexed.record()) :: :ok
  def put(%{managed: managed_state}, name, record) do
    put(managed_state, name, record)
  end

  def put(%{index: index}, name, record) do
    Indexed.get(index, name, record)
  end

  @spec drop(state_or_wrapped, atom, Indexed.id()) :: :ok | :error
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

  @spec get_records(state_or_wrapped, atom, Indexed.prefilter(), Indexed.order_hint() | nil) ::
          [Indexed.record()] | nil
  def get_records(state, name, prefilter, order_hint \\ nil)

  def get_records(%{managed: managed_state}, name, prefilter, order_hint) do
    get_records(managed_state, name, prefilter, order_hint)
  end

  def get_records(%{index: index}, name, prefilter, order_hint) do
    Indexed.get_records(index, name, prefilter, order_hint)
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

  # @spec do_manage_child(State.t(), Ecto.Association.t(), map | nil, map | nil) :: State.t()
  # def do_manage_child(%{module: mod} = state, %BelongsTo{cardinality: :one} = assoc, orig, new) do
  #   %{field: field, owner_key: owner_key, related: related} = assoc
  #   # |> log(label: "SHIT")

  #   %{get_fn: get_fn, module: module, name: name} = assoc_managed = get_managed(mod, related)

  #   # log(orig, label: "oooooOOORIG #{field} - #{owner_key}")
  #   # log(new, label: "oooooNNNEW #{field} - #{owner_key}")

  #   assoc_orig =
  #     with %NotLoaded{} <- orig && Map.get(orig, field) do
  #       orig
  #       |> Managed.preload(state, field)
  #       # |> IO.inspect(label: "pppppp")
  #       |> Map.get(field)

  #       # |> log(label: "LLLOADED ORIG")
  #     end

  #   # assoc_new = new && Map.get(new, field)
  #   assoc_new =
  #     with %NotLoaded{} <- new && Map.get(new, field) do
  #       # IO.inspect({get_fn, new, Map.get(new, owner_key)}, label: "CHECKCHECK")

  #       case get_fn && new && Map.get(new, owner_key) do
  #         nil -> nil
  #         id -> get_fn.(id)
  #       end

  #       # |> log(label: "LLLOADED NEW")
  #     end

  #   # log(assoc_orig, label: "ASSOC ORIG")
  #   # log(assoc_new, label: "ASSOC NEW")

  #   track = &do_track(&1, &2, name, assoc, orig, new)
  #   mod_or_nil? = fn it, m -> match?(%^m{}, it) or is_nil(it) end

  #   if mod_or_nil?.(assoc_orig, module) and mod_or_nil?.(assoc_new, module) and
  #        not (is_nil(assoc_orig) and is_nil(assoc_new)) do
  #     # log(label: "WORD1")
  #     # Assoc in orig/new was preloaded. Manage recursively.
  #     state = if tracked?(assoc_managed), do: track.(state, false), else: state
  #     manage(state, name, assoc_orig, assoc_new)
  #   else
  #     # log(label: "WORD2")
  #     if tracked?(assoc_managed), do: track.(state, true), else: state
  #   end
  # end

  # def do_manage_child(%{module: mod} = state, %Has{cardinality: :many} = assoc, orig, new) do
  #   %{field: field, related: related, related_key: related_key} = assoc
  #   %{name: related_name, module: related_module} = get_managed(mod, related)

  #   log("ok: #{inspect({field, related_name})}")

  #   assoc_orig =
  #     with %NotLoaded{} <- orig && Map.get(orig, field) do
  #       orig
  #       |> Managed.preload(state, field)
  #       |> Map.get(field)
  #       |> log(label: "LOADED ORIG")
  #     end
  #     |> log(label: "ORIG")

  #   # If the assoc isn't loaded in the new record, load it.
  #   # First try a custom function defined in :children_getters.
  #   # If none, use a generic fetch-by-foreign-key approach.
  #   assoc_new =
  #     with %NotLoaded{} <- new && Map.get(new, field) do
  #       case get_in(get_managed(mod, new.__struct__), [
  #              Access.key(:children_getters),
  #              related_name
  #            ]) do
  #         {fn_mod, fn_name} ->
  #           apply(fn_mod, fn_name, [new.id])

  #         nil ->
  #           new_id = new.id
  #           # TODOO
  #           state.repo.all(from r in related_module, where: field(r, ^related_key) == ^new_id)
  #       end
  #     end

  #   do_manage_has_many(state, related_name, assoc_orig, assoc_new)
  # end

  # # Update in-state tracking and maybe
  # @spec do_track(State.t(), boolean, atom, Ecto.Association.t(), map | nil, map | nil) ::
  #         State.t()
  # defp do_track(state, fetch_record?, name, %{owner_key: owner_key} = assoc, orig, new) do
  #   orig_id = orig && Map.get(orig, owner_key)
  #   new_id = new && Map.get(new, owner_key)
  #   state = state |> add_tracked(name, new_id) |> rm_tracked(name, orig_id)
  #   if fetch_record?, do: do_tracked_record(state, name, assoc, orig, new), else: state
  # end

  # # Add or remove the tracked record in the cache, based on a change to its
  # # parent, `orig` and `new`.

  # # `orig` or `new` may be `nil` to indicate that the record has been created or
  # # deleted. If no change is needed, none is made. `state` (tracking maps) must
  # # already reflect the change being made, so `update_tracked_for/4` or similar
  # # must be called first.

  # # For the first instance of the association, it will be loaded via `:get_fn`.
  # @spec do_tracked_record(State.t(), atom, Ecto.Association.t(), map | nil, map | nil) ::
  #         State.t()
  # defp do_tracked_record(%{module: mod, tracking: tracking} = state, name, assoc, orig, new) do
  #   %{field: field, owner_key: owner_key, related: related} = assoc
  #   orig_tracked_id = orig && Map.get(orig, owner_key)
  #   new_tracked_id = new && Map.get(new, owner_key)
  #   tracking_map = Map.fetch!(tracking, name)
  #   tracking_count = tracking_map[new_tracked_id]

  #   state =
  #     if new_tracked_id && tracking_count == 1 do
  #       r1 = orig && Map.get(orig, field)
  #       r2 = new && Map.get(new, field)
  #       record = do_get_record(r1, r2, related, mod, name, new_tracked_id)
  #       manage(state, name, r1, record)
  #     else
  #       state
  #     end

  #   if orig_tracked_id && is_nil(tracking_count),
  #     do: Indexed.drop(state.index, name, orig_tracked_id)

  #   state
  # end

  # # Load an associated record.
  # #
  # # - First see if `new` is an already-loaded instance.
  # # - If not, use the `get_fn` on the managed configuration.
  # # - If that function isn't defined, raise -- it should be!
  # @spec do_get_record(any, any, module, module, atom, any) :: map
  # defp do_get_record(_, %related{} = new, related, _, _, _) do
  #   new
  # end

  # defp do_get_record(_, _, _, mod, name, new_tracked_id) do
  #   %{get_fn: get_fn} = mod.__tracked__(name)
  #   get_fn || raise ":get_fn not defined on #{name} managed declaration."
  #   get_fn.(new_tracked_id)
  # end

  # # For a managed record's managed children, call manage on each as needed.
  # @spec do_manage_has_many(State.t(), atom, [map] | nil, [map] | nil) :: State.t()
  # defp do_manage_has_many(state, entity_name, orig_list, new_list)
  #      when is_list(orig_list) and is_list(new_list) do
  #   # Manage the record if it's remained in the list or if it was removed.
  #   state =
  #     Enum.reduce(orig_list, state, fn orig_record, acc ->
  #       new_record = Enum.find(new_list, &(&1.id == orig_record.id))

  #       if new_record,
  #         do: manage(acc, entity_name, orig_record, new_record),
  #         else: manage(acc, entity_name, orig_record, nil)
  #     end)

  #   # Manage the record if it's been added to the list.
  #   Enum.reduce(new_list, state, fn new_record, acc ->
  #     if Enum.any?(orig_list, &(&1.id == new_record.id)),
  #       do: acc,
  #       else: manage(acc, entity_name, nil, new_record)
  #   end)
  # end

  # defp do_manage_has_many(state, entity_name, orig_list, nil) when is_list(orig_list) do
  #   Enum.reduce(orig_list, state, &manage(&2, entity_name, &1, nil))
  # end

  # defp do_manage_has_many(state, entity_name, nil, new_list) when is_list(new_list) do
  #   Enum.reduce(new_list, state, &manage(&2, entity_name, nil, &1))
  # end

  # defp do_manage_has_many(state, _, _, _), do: state

  # @doc """
  # For each tracked child of the entity in `orig` and `new`, update the tracking.

  # This is useful when warming the `Indexed` cache when you'd prefer to fetch all
  # the records at once and then iterate through to update the `t:tracking/0`.
  # """
  # @spec update_tracked_for(State.t(), atom, map | nil, map | nil) :: State.t()
  # def update_tracked_for(%{module: mod} = state, name, orig, new) do
  #   %{children: children, module: module} = get_managed(mod, name)

  #   Enum.reduce(children, state, fn {field, _preload_spec}, acc ->
  #     %{owner_key: owner_key, related: related} = module.__schema__(:association, field)
  #     managed = get_managed(mod, related)

  #     if tracked?(managed) do
  #       orig_id = orig && Map.get(orig, owner_key)
  #       new_id = new && Map.get(new, owner_key)
  #       acc |> add_tracked(managed.name, new_id) |> rm_tracked(managed.name, orig_id)
  #     else
  #       acc
  #     end
  #   end)
  # end

  @doc "Add a new id to `name` tracking. Subscribe if it's new."
  @spec add_tracked(State.t(), atom, any) :: State.t()
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
  @spec get_managed(State.t() | module, atom) :: Managed.t()
  defp get_managed(%{module: mod}, name), do: get_managed(mod, name)

  defp get_managed(mod, name) do
    mod.__managed__(name) ||
      raise ":#{name} must have a managed declaration on #{inspect(mod)}."
  end

  # Get the indexing "id" of a particular managed record.
  @spec id(t | id_key | nil, map) :: any
  defp id(%{id_key: id_key}, record), do: id(id_key, record)
  defp id(id_key, record) when is_function(id_key), do: id_key.(record)
  defp id(nil, record), do: raise("No id_key found for #{inspect(record)}")
  defp id(id_key, record), do: Map.get(record, id_key)

  # @spec id(module, atom, map) :: any
  # defp id(mod, name, record) do
  #   mod |> get_managed(name) |> Map.get(:id_key) |> id(record)
  # end

  def tracked?(_), do: true
  # @doc "Given a `t:Managed.t/0`, return true if it is tracked."
  # @spec tracked?(Managed.t()) :: boolean
  # def tracked?(%{subscribe: sf}) when is_function(sf),
  #   do: true

  # def tracked?(_), do: false

  # @doc "Given a module and entity name, return true if it is tracked."
  # def tracked?(module, entity_name) do
  #   tracked?(get_managed(module, entity_name))
  # end

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