defmodule User do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "users" do
    field :name, :string
    timestamps()
  end

  def changeset(struct_or_changeset, params) do
    struct_or_changeset
    |> cast(params, [:name])
    |> validate_required([:name])
  end
end
