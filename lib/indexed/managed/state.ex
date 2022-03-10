defmodule Indexed.Managed.State do
  @moduledoc "A piece of GenServer state for Managed."
  alias __MODULE__
  defstruct [:index, :module, :repo, :tmp, :tracking]

  @typedoc """
  Data structure used to hold temporary data while running `manage/5`.

  # * `:add_ids` - For an entity name, the inner map uses tuple keys with the
  #   entity id and its plural-named field with a many association. These map to a
  #   list of ids which will be added in the "add" pass.
  * `:keep` - Map of entity names to lists of IDs for records which may
    otherwise be deleted because refs are 0 but in fact should NOT be deleted
    because we are holding another record with a has_many relationship.
  * `:records` - Outer map is keyed by entity name. Inner map is keyed by
    record id. Values are the records themselves. These are new records which
    may be committed to ETS at the end of the operation.
  * `:tracking` - For record ids relevant to the operation, initial values are
    copied from State and manipulated as needed within this structure.
  """
  @type tmp :: %{
          keep: %{atom => [id]},
          records: %{atom => %{id => record}},
          rm_ids: %{atom => %{id => %{atom => [id]}}},
          top_rm_ids: %{atom => [id]},
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
  @spec init_tmp(t) :: t
  def init_tmp(%{module: mod} = state) do
    records = Map.new(mod.__tracked__(), &{&1, %{}})

    %{
      state
      | tmp: %{
          keep: %{},
          records: records,
          rm_ids: %{},
          top_rm_ids: %{},
          tracking: init_tracking(mod)
        }
    }
  end

  @spec init_tracking(module) :: map
  defp init_tracking(mod), do: Map.new(mod.__tracked__(), &{&1, %{}})
end
