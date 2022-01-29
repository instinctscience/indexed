defmodule Indexed.Test.Repo.Migrations.CreateCar do
  use Ecto.Migration

  def change do
    create table(:albums) do
      add(:artist, :string)
      add(:label, :string)
      add(:media, :string)
    end

    create table(:users) do
      add(:name, :string)
      timestamps()
    end

    create table(:posts) do
      add(:author_id, references(:users))
      add(:content, :string)
      timestamps()
    end

    create table(:comments) do
      add(:author_id, references(:users))
      add(:content, :string)
      add(:post_id, references(:posts))
      timestamps()
    end
  end
end
