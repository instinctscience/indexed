defmodule IndexedViewsTest do
  @moduledoc ~S/Test "view" functionality./
  use ExUnit.Case

  # (Who needs album names?)
  @albums [
    %Album{id: 1, label: "Liquid V Recordings", media: "Vinyl", artist: "Calibre"},
    %Album{id: 2, label: "Hospital Records", media: "CD", artist: "Logistics"},
    %Album{id: 3, label: "Hospital Records", media: "FLAC", artist: "London Elektricity"},
    %Album{id: 4, label: "Liquid V Recordings", media: "CD", artist: "Roni Size"},
    %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
  ]

  setup do
    params = [label: "Hospital Records", starts_with: "Lo"]
    print = "eb36c402b810b2cdc87bbaec"

    index =
      Indexed.warm(
        albums: [
          data: {:asc, :artist, @albums},
          fields: [:artist, :media],
          prefilters: [
            nil: [maintain_unique: [:media]],
            label: [maintain_unique: [:media]]
          ]
        ]
      )

    view =
      Indexed.create_view(index, :albums, print,
        prefilter: {:label, "Hospital Records"},
        maintain_unique: [:id],
        filter: &String.starts_with?(&1.artist, "Lo")
      )

    [fingerprint: print, index: index, params: params, view: view]
  end

  describe "fingerprint" do
    test "typical", %{fingerprint: fingerprint, params: params} do
      assert fingerprint == Indexed.fingerprint(params)
    end

    test "list param", %{params: params} do
      assert "cbcb293fbd4803362aa6b18d" == Indexed.fingerprint([{:fooz, [1, 2]} | params])
    end
  end

  test "get view", %{fingerprint: fingerprint, index: index, view: view} do
    expected_view = %Indexed.View{
      maintain_unique: [:id],
      prefilter: {:label, "Hospital Records"},
      filter: view.filter
    }

    assert expected_view == view
    assert expected_view == Indexed.get_view(index, :albums, fingerprint)
    assert is_function(view.filter)
  end

  describe "just warmed up" do
    test "get view records", %{fingerprint: fingerprint, index: index} do
      a1 = %Album{artist: "Logistics", id: 2, label: "Hospital Records", media: "CD"}
      a2 = %Album{artist: "London Elektricity", id: 3, label: "Hospital Records", media: "FLAC"}

      assert [a1, a2] == Indexed.get_records(index, :albums, fingerprint, :artist, :asc)
      assert %{2 => 1, 3 => 1} == Indexed.get_uniques_map(index, :albums, fingerprint, :id)
      assert [2, 3] == Indexed.get_uniques_list(index, :albums, fingerprint, :id)

      assert [a2, a1] == Indexed.get_records(index, :albums, fingerprint, :media, :desc)
    end
  end

  describe "with a record updated" do
    test "record is removed from the view", %{fingerprint: fingerprint, index: index} do
      album = %Album{id: 3, label: "Hospital Records", media: "FLAC", artist: "Whiney"}
      Indexed.put(index, :albums, album)

      assert [%Album{artist: "Logistics", id: 2, label: "Hospital Records", media: "CD"}] ==
               Indexed.get_records(index, :albums, fingerprint, :artist, :asc)
    end
  end
end
