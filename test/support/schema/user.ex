defmodule User do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "users" do
    has_many :flare_pieces, FlarePiece
    field :name, :string
    timestamps()
  end

  @type t :: %__MODULE__{}

  def changeset(struct_or_changeset, params) do
    struct_or_changeset
    |> cast(params, [:name])
    |> validate_required([:name])
    |> cast_assoc(:flare_pieces)
  end
end
