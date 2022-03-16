defmodule Indexed.Managed.State do
  @moduledoc "A piece of GenServer state for Managed."
  alias __MODULE__

  defstruct [:index, :module, :repo, :tmp, :tracking]

  @typedoc """
  Data structure used to hold temporary data while running `manage/5`.

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
          records: %{atom => %{id => record}},
          rm_ids: %{atom => %{id => %{atom => [id]}}},
          top_name: atom,
          top_rm_ids: [id],
          tracking: tracking
        }

  @typedoc """
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
  Map of tracked record ids to occurrences throughout the records held.
  Used to manage subscriptions.
  """
  @type tracking_status :: %{id => non_neg_integer}

  @typep id :: Indexed.id()
  @typep record :: Indexed.record()

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init(module, module) :: t
  def init(mod, repo) do
    %State{module: mod, repo: repo, tracking: init_tracking(mod)}
  end

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init_tmp(t, atom) :: t
  def init_tmp(%{module: mod} = state, name) do
    records = Map.new(mod.__tracked__(), &{&1, %{}})

    %{
      state
      | tmp: %{
          records: records,
          rm_ids: %{},
          top_name: name,
          top_rm_ids: [],
          tracking: init_tracking(mod)
        }
    }
  end

  @spec init_tracking(module) :: map
  defp init_tracking(mod), do: Map.new(mod.__tracked__(), &{&1, %{}})
end
