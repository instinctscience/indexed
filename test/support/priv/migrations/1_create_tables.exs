defmodule Indexed.Test.Repo.Migrations.CreateCar do
  use Ecto.Migration

  def change do
    create table(:albums) do
      add(:artist, :string)
      add(:label, :string)
      add(:media, :string)
    end
  end
end
