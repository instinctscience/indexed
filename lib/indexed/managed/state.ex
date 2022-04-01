defimpl Inspect, for: Indexed.Managed.State do
  def inspect(_state, _opts) do
    "#Indexed.Managed.State<>"
  end
end

defmodule Indexed.Managed.State do
  @moduledoc "A piece of GenServer state for Managed."
  alias Indexed.Managed
  alias __MODULE__

  defstruct [:index, :module, :repo, :tmp, :tracking]

  @typedoc """
  Data structure used to hold temporary data while running `manage/5`.

  * `:done_ids` - ...Entity name-keyed map with lists of record IDs which have been
    processed during the `:add` or `:remove` phases because another entity has
    :one of them. This allows us to skip processing it next time(s) if it
    appears elsewhere. Emptied between phases.
  * `:one_rm_ids` - ...
  * `:one_rm_queue` - these ahve already been removed themselves, OR Ocosmething
  * `:records` - Records which may be committed to ETS at the end of the
    operation. Outer map is keyed by entity name. Inner map is keyed by record
    id.
  * `:rm_ids` - Record IDs queued for removal with respect to their parent.
    Outer map is keyed by entity name. Inner map is keyed by parent ID.
    Inner-most map is keyed by parent field containing the children.
  * `:top_rm_ids` - Top-level record IDs queued for removal.
  * `:tracking` - For record ids relevant to the operation, initial values are
    copied from State and manipulated as needed within this structure.
  """
  @type tmp :: %{
          # done_ids: %{atom => %{id => {rm_phase :: boolean, add_phase :: boolean}}},
          done_ids: %{atom => %{phase => [id]}},
          # one_rm_ids: %{atom => [id]},
          # one_rm_queue: %{atom => %{list => %{id => record}}},
          one_rm_queue: %{atom => %{id => {list, record}}},
          records: %{atom => %{id => record}},
          rm_ids: %{atom => %{id => %{atom => [id]}}},
          top_rm_ids: [id],
          tracking: tracking
        }

  @typep phase :: :add | :rm

  @typedoc """
  * `:index` - Indexed struct. Static after created via `Indexed.warm/1`.
  * `:module` - Module which has `use Indexed.Managed`.
  * `:repo` - Ecto Repo module to use for loading data.
  * `:tmp` - Data structure used internally during a call to `manage/5`.
    Otherwise `nil`.
  * `:tracking` - Data about how many :one refs there are to a certain entity.
  """
  @type t :: %State{
          index: Indexed.t() | nil,
          module: module,
          repo: module,
          tmp: tmp | nil,
          tracking: tracking
        }

  @typedoc """
  A set of tracked entity statuses.

  An entity is tracked if another entity refers to it with a :one relationship.
  """
  @type tracking :: %{atom => tracking_status}

  @typedoc """
  Map of tracked record IDs to occurrences throughout the records held.
  Used to manage subscriptions and garbage collection.
  """
  @type tracking_status :: %{id => non_neg_integer}

  @typep id :: Indexed.id()
  @typep record :: Indexed.record()

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init(module, module) :: t
  def init(mod, repo) do
    %State{module: mod, repo: repo, tracking: empty_tracking_map(mod)}
  end

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init_tmp(t) :: t
  def init_tmp(%{module: mod} = state) do
    empty = empty_tracking_map(mod)

    %{
      state
      | tmp: %{
          done_ids: empty,
          # one_rm_ids: tracking_w_list,
          one_rm_queue: empty,
          records: empty,
          rm_ids: %{},
          top_rm_ids: [],
          tracking: empty
        }
    }
  end

  # Make a map of maps keyed by all tracked entity names.
  @spec empty_tracking_map(module) :: map
  defp empty_tracking_map(mod), do: Map.new(mod.__tracked__(), &{&1, %{}})

  @spec one_rm_queue(t, atom) :: %{id => tuple}
  def one_rm_queue(state, name) do
    get_in(state, [Access.key(:tmp), :one_rm_queue, name])
  end

  # def add_one_rm_ids(state, name, ids) do
  #   fun = &Kernel.++(&1, ids)
  #   update_in(state, [Access.key(:tmp), :one_rm_ids, name], fun)
  # end

  @doc "Add a set of records into tmp's `:one_rm_queue`."
  @spec add_one_rm_queue(t, atom, list, %{id => record}) :: t
  def add_one_rm_queue(state, name, sub_path, record_map) do
    update_in(state, [Access.key(:tmp), :one_rm_queue, name], fn of_entity ->
      Enum.reduce(record_map, of_entity || %{}, fn {id, rec}, acc ->
        Map.put(acc, id, {sub_path, rec})
      end)
    end)
  end

  # @doc "Reset `:done_ids` inside `:tmp`."
  # @spec reset_tmp_done_ids(t) :: t
  # def reset_tmp_done_ids(%{module: mod} = state) do
  #   put_in(state, [Access.key(:tmp), :done_ids], tracking_keyed_map(mod))
  # end

  @doc "Get `ids` list for `name` in tmp's done_ids."
  @spec tmp_done_ids(t, atom, phase) :: [id]
  def tmp_done_ids(state, name, phase) do
    get_in(state, [Access.key(:tmp), :done_ids, name, phase]) || []
  end

  @doc "Add `ids` list for `name` in tmp's done_ids."
  @spec add_tmp_done_ids(t, atom, phase, [id]) :: t
  def add_tmp_done_ids(state, name, phase, ids) do
    update_in(state, [Access.key(:tmp), :done_ids, name], fn
      %{^phase => existing_ids} = map -> Map.put(map, phase, existing_ids ++ ids)
      %{} = map -> Map.put(map, phase, ids)
      nil -> %{phase => ids}
    end)
  end

  # Drop from the index all records in
  @spec drop_rm_ids(t) :: :ok
  def drop_rm_ids(%{module: mod, tmp: %{rm_ids: rm_ids}} = state) do
    Enum.each(rm_ids, fn {parent_name, map} ->
      Enum.each(map, fn {_parent_id, map2} ->
        Enum.each(map2, fn {path_entry, ids} ->
          with %{^path_entry => {:many, name, _, _}} <- mod.__managed__(parent_name).children,
               do: Enum.each(ids, &Managed.drop(state, name, &1))
        end)
      end)
    end)
  end

  # Drop from the index all records in tmp.top_rm_ids.
  @spec drop_top_rm_ids(t, atom) :: :ok
  def drop_top_rm_ids(%{tmp: %{top_rm_ids: ids}} = state, name) do
    Enum.each(ids, &Managed.drop(state, name, &1))
  end

  # Remove an assoc id from tmp.rm_ids.
  @spec subtract_tmp_rm_id(t, Managed.parent_info(), id) :: t
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
  @spec add_tmp_rm_id(t, Managed.parent_info(), id) :: t
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

  # Get the tracking (number of references) for the given entity and id.
  @spec tracking(t, atom, any) :: non_neg_integer
  def tracking(%{tracking: tracking}, name, id),
    do: get_in(tracking, [name, id]) || 0

  # Get the tmp tracking (number of references) for the given entity and id.
  @spec tmp_tracking(t, atom, any) :: non_neg_integer
  def tmp_tracking(%{tmp: %{tracking: tt}, tracking: t}, name, id) do
    get = &get_in(&1, [name, id])
    get.(tt) || get.(t) || 0
  end

  # Update tmp tracking. If a function is given, its return value will be used.
  # As input, the fun gets the current count, using non-tmp tracking if empty.
  @spec put_tmp_tracking(t, atom, id, non_neg_integer | (non_neg_integer -> non_neg_integer)) :: t
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
end
