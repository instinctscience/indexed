defimpl Inspect, for: Indexed.Managed.State do
  def inspect(_state, _opts) do
    "#Indexed.Managed.State<>"
  end
end

defmodule Indexed.Managed.State do
  @moduledoc "A piece of GenServer state for Managed."
  alias __MODULE__

  defstruct [:index, :module, :repo, :tmp, :tracking]

  @typedoc """
  Data structure used to hold temporary data while running `manage/5`.

  * `:done_ids` - Entity name-keyed map with lists of record IDs which have been
    processed during the `:add` or `:remove` phases because another entity has
    :one of them. This allows us to skip processing it next time(s) if it
    appears elsewhere. Only used during a phase -- emptied in between.
  * `:records` - Records which may be committed to ETS at the end of the
    operation. Outer map is keyed by entity name. Inner map is keyed by record
    id.
  * `:rm_ids` - Record IDs queued for removal with respect to their parent.
    Outer map is keyed by entity name. Inner map is keyed by parent ID.
    Inner-most map is keyed by parent field containing the children.
  * `:top_name` - Entity name of the top-level records.
  * `:top_rm_ids` - Top-level record IDs queued for removal.
  * `:tracking` - For record ids relevant to the operation, initial values are
    copied from State and manipulated as needed within this structure.
  """
  @type tmp :: %{
          done_ids: %{atom => [id]},
          records: %{atom => %{id => record}},
          rm_ids: %{atom => %{id => %{atom => [id]}}},
          top_name: atom,
          top_rm_ids: [id],
          tracking: tracking
        }

  @typedoc """
  * `:index` - Indexed struct. Static after created via `Indexed.warm/1`.
  * `:module` - Module which has `use Indexed.Managed`.
  * `:repo` - Ecto Repo module to use for loading data.
  * `:tmp` - Data structure used internally during a call to `manage/5`.
    Usually `nil`.
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
    %State{module: mod, repo: repo, tracking: tracking_keyed_map(mod)}
  end

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init_tmp(t, atom) :: t
  def init_tmp(%{module: mod} = state, name) do
    empty = tracking_keyed_map(mod)

    %{
      state
      | tmp: %{
          done_ids: empty,
          records: empty,
          rm_ids: %{},
          top_name: name,
          top_rm_ids: [],
          tracking: tracking_keyed_map(mod)
        }
    }
  end

  @doc "Reset `:done_ids` inside `:tmp`."
  @spec reset_tmp_done_ids(t) :: t
  def reset_tmp_done_ids(%{module: mod} = state) do
    put_in(state, [Access.key(:tmp), :done_ids], tracking_keyed_map(mod))
  end

  @doc "Get `ids` list for `module` in tmp's done_ids."
  @spec tmp_done_ids(t, module) :: [id]
  def tmp_done_ids(state, module) do
    get_in(state, [Access.key(:tmp), :done_ids, module]) || []
  end

  @doc "Add `ids` list for `module` in tmp's done_ids."
  @spec add_tmp_done_ids(t, module, [id]) :: t
  def add_tmp_done_ids(state, module, ids) do
    fun = &Kernel.++(&1 || [], ids)
    update_in(state, [Access.key(:tmp), :done_ids, module], fun)
  end

  # Make a map of maps keyed by all tracked entity names.
  @spec tracking_keyed_map(module) :: map
  defp tracking_keyed_map(mod), do: Map.new(mod.__tracked__(), &{&1, %{}})
end
