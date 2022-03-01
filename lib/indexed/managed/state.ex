defmodule Indexed.Managed.State do
  @moduledoc "A piece of GenServer state for Managed."
  alias Indexed.Managed
  alias __MODULE__
  defstruct [:index, :module, :repo, :tmp, :tracking]

  @typedoc """
  Data structure used to hold temporary data while running an operation.

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
          keep: %{atom => [Indexed.id()]},
          records: %{atom => %{Indexed.id() => Indexed.record()}},
          tracking: Managed.tracking()
        }

  @typedoc """
  """
  @type t :: %State{
          index: Indexed.t() | nil,
          module: module,
          repo: module,
          tmp: tmp | nil,
          tracking: Managed.tracking()
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
    %{state | tmp: %{keep: %{}, records: records, tracking: init_tracking(mod)}}
  end

  @spec init_tracking(module) :: map
  defp init_tracking(mod), do: Map.new(mod.__tracked__(), &{&1, %{}})
end
