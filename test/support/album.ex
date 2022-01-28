defmodule Album do
  @moduledoc false
  use Ecto.Schema

  schema "albums" do
    field :artist, :string
    field :label, :string
    field :media, :string
  end
end
