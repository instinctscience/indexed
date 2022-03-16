defmodule Indexed.Managed.Helpers do
  @moduledoc "Some tools for `Indexed.Managed`."
  alias Ecto.Association.NotLoaded
  alias Indexed.Managed, as: M

  @typep assoc_spec :: M.assoc_spec()
  @typep id :: Indexed.id()
  @typep parent_info :: M.parent_info()
  @typep record :: Indexed.record()
  @typep state :: M.State.t()

  @doc """
  Invoke fun with the managed state, finding it in the :managed key if needed.
  If fun returns a managed state and it was wrapped, rewrap it.
  """
  @spec with_state(M.state_or_wrapped(), (state -> any)) :: any
  def with_state(%{managed: state} = wrapper, fun) do
    with %M.State{} = new_managed <- fun.(state),
         do: %{wrapper | managed: new_managed}
  end

  def with_state(state, fun), do: fun.(state)

  # Returns true if we're holding in cache
  # another record with a has_many including the record for match_id.
  @spec has_referring_many?(state, atom, id) :: boolean
  def has_referring_many?(%{module: mod} = state, match_name, match_id) do
    Enum.any?(mod.__managed__(), fn name ->
      Enum.any?(mod.__managed__(name).children, fn
        {:many, ^match_name, prefilter_key, _} ->
          match_id == Map.fetch!(M.get(state, match_name, match_id), prefilter_key)

        _ ->
          false
      end)
    end)
  end

  # Drop from the index all records in
  @spec drop_rm_ids(state) :: :ok
  def drop_rm_ids(%{module: mod, tmp: %{rm_ids: rm_ids}} = state) do
    Enum.each(rm_ids, fn {parent_name, map} ->
      Enum.each(map, fn {_parent_id, map2} ->
        Enum.each(map2, fn {path_entry, ids} ->
          with %{^path_entry => {:many, name, _, _}} <- mod.__managed__(parent_name).children,
               do: Enum.each(ids, &M.drop(state, name, &1))
        end)
      end)
    end)
  end

  # Drop from the index all records in tmp.top_rm_ids.
  @spec drop_top_rm_ids(state) :: :ok
  def drop_top_rm_ids(%{tmp: %{top_name: name, top_rm_ids: ids}} = state) do
    Enum.each(ids, &M.drop(state, name, &1))
  end

  # Remove an assoc id from tmp.rm_ids.
  @spec subtract_tmp_rm_id(state, parent_info, id) :: state
  def subtract_tmp_rm_id(state, :top, id) do
    update_in(state, [Access.key(:tmp), :top_rm_ids], fn
      nil -> []
      l -> l -- [id]
    end)
  end

  def subtract_tmp_rm_id(state, parent_info, id) do
    update_in_tmp_rm_id(state, parent_info, &(&1 -- [id]))
  end

  # Add an assoc id into tmp.rm_ids.
  @spec add_tmp_rm_id(state, parent_info, id) :: state
  def add_tmp_rm_id(state, :top, id) do
    update_in(state, [Access.key(:tmp), :top_rm_ids], &[id | &1 || []])
  end

  def add_tmp_rm_id(state, parent_info, id) do
    update_in_tmp_rm_id(state, parent_info, &[id | &1 || []])
  end

  def update_in_tmp_rm_id(state, {a, b, c}, fun) do
    m = &Access.key(&1, %{})
    keys = [m.(a), m.(b), Access.key(c, [])]
    rm_ids = update_in(state.tmp.rm_ids, keys, fun)
    put_in(state, [Access.key(:tmp), :rm_ids], rm_ids)
  end

  # Get the foreign key for the `path_entry` field of `module`.
  @spec get_fkey(module, atom) :: atom
  def get_fkey(module, path_entry) do
    module.__schema__(:association, path_entry).related_key
  end

  # Wrap ecto query if `:query` function is defined.
  @spec build_query(M.t()) :: Ecto.Queryable.t()
  def build_query(%{module: assoc_mod, query: nil}),
    do: assoc_mod

  def build_query(%{module: assoc_mod, query: query_fn}),
    do: query_fn.(assoc_mod)

  # Get the tracking (number of references) for the given entity and id.
  @spec tracking(map, atom, any) :: non_neg_integer
  def tracking(%{tracking: tracking}, name, id),
    do: get_in(tracking, [name, id]) || 0

  # Get the tmp tracking (number of references) for the given entity and id.
  @spec tmp_tracking(map, atom, any) :: non_neg_integer
  def tmp_tracking(%{tmp: %{tracking: tt}, tracking: t}, name, id) do
    get = &get_in(&1, [name, id])
    get.(tt) || get.(t) || 0
  end

  # Update tmp tracking. If a function is given, its return value will be used.
  # As input, the fun gets the current count, using non-tmp tracking if empty.
  @spec put_tmp_tracking(state, atom, id, non_neg_integer | (non_neg_integer -> non_neg_integer)) ::
          state
  def put_tmp_tracking(state, name, id, num_or_fun) when is_function(num_or_fun) do
    update_in(state, [Access.key(:tmp), :tracking, name, id], fn
      nil ->
        num = Map.fetch!(state.tracking, name)[id] || 0
        num_or_fun.(num)

      num ->
        num_or_fun.(num)
    end)
  end

  def put_tmp_tracking(state, name, id, num_or_fun),
    do: put_tmp_tracking(state, name, id, fn _ -> num_or_fun end)

  def put_tmp_record(state, name, id, record),
    do: put_in(state, [Access.key(:tmp), :records, name, id], record)

  # Attempt to lift an association directly from its parent.
  @spec assoc_from_record(record, atom) :: record | nil
  def assoc_from_record(record, path_entry) do
    case record do
      %{^path_entry => %NotLoaded{}} -> nil
      %{^path_entry => %{} = assoc} -> assoc
      _ -> nil
    end
  end

  # Invoke :subscribe function for the given entity id if one is defined.
  @spec maybe_subscribe(module, atom, id) :: any
  def maybe_subscribe(mod, name, id) do
    with %{subscribe: sub} when is_function(sub) <- get_managed(mod, name),
         do: sub.(id)
  end

  # Invoke :unsubscribe function for the given entity id if one is defined.
  @spec maybe_unsubscribe(module, atom, id) :: any
  def maybe_unsubscribe(mod, name, id) do
    with %{unsubscribe: usub} when is_function(usub) <- get_managed(mod, name),
         do: usub.(id)
  end

  # Get the %Managed{} or raise an error.
  @spec get_managed(state | module, atom) :: M.t()
  def get_managed(%{module: mod}, name), do: get_managed(mod, name)

  def get_managed(mod, name) do
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
      M.get(state, name, Map.get(record, key))
    end
  end

  def preload_fn({:many, name, pf_key, order_hint}, _repo) do
    fn record, state ->
      pf = if pf_key, do: {pf_key, record.id}, else: nil
      M.get_records(state, name, pf, order_hint) || []
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

  # Unload all associations (or only `assocs`) in an ecto schema struct.
  @spec drop_associations(struct, [atom] | nil) :: struct
  def drop_associations(%mod{} = schema, assocs \\ nil) do
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
