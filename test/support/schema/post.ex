defmodule Post do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "posts" do
    belongs_to :author, User
    has_many :comments, Comment
    field :content, :string
    timestamps()
  end

  def changeset(struct_or_changeset, params) do
    struct_or_changeset
    |> cast(params, [:author_id, :content])
    |> validate_required([:author_id, :content])
  end
end
