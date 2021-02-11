defmodule Album do
  defstruct [:id, :label, :media, :artist]
end

defmodule IndexedPrefilterTest do
  @moduledoc "Test `:prefilter` and `:maintain_unique` options."
  use ExUnit.Case

  @albums [
    %Album{id: 1, label: "Liquid V Recordings", media: "Vinyl", artist: "Calibre"},
    %Album{id: 2, label: "Hospital Records", media: "CD", artist: "Logistics"},
    %Album{id: 3, label: "Hospital Records", media: "FLAC", artist: "London Elektricity"},
    %Album{id: 4, label: "Liquid V Recordings", media: "CD", artist: "Roni Size"},
    %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
  ]

  setup do
    [
      index:
        Indexed.warm(
          albums: [
            data: {:asc, :artist, @albums},
            fields: [:artist],
            prefilters: [nil: [maintain_unique: [:media]], label: [maintain_unique: [:media]]]
          ]
        )
    ]
  end

  test "basic prefilter", %{index: index} do
    assert %Paginator.Page{
             entries: [
               %Album{id: 2, label: "Hospital Records", media: "CD", artist: "Logistics"},
               %Album{
                 id: 3,
                 label: "Hospital Records",
                 media: "FLAC",
                 artist: "London Elektricity"
               },
               %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
             ],
             metadata: %Paginator.Page.Metadata{
               after: nil,
               before: nil,
               limit: 10,
               total_count: nil,
               total_count_cap_exceeded: false
             }
           } ==
             Indexed.paginate(index, :albums,
               order_field: :artist,
               order_direction: :asc,
               prefilter: {:label, "Hospital Records"}
             )
  end

  test "get_unique_values", %{index: index} do
    assert ["Hospital Records", "Liquid V Recordings"] ==
             Indexed.get_unique_values(index, :albums, :label)

    assert ~w(CD FLAC Vinyl) ==
             Indexed.get_unique_values(index, :albums, :media)

    assert ~w(CD FLAC) ==
             Indexed.get_unique_values(index, :albums, :media, {:label, "Hospital Records"})
  end

  describe "looks good after adding a record" do
    setup %{index: index} do
      album = %{id: 6, label: "Hospital Records", media: "Minidisc", artist: "Bop"}
      Indexed.set_record(index, :albums, album)
      [album: album]
    end

    test "basic prefilter", %{album: album, index: index} do
      assert %Paginator.Page{
               entries: [
                 ^album,
                 %Album{id: 2, label: "Hospital Records", media: "CD", artist: "Logistics"},
                 %Album{
                   id: 3,
                   label: "Hospital Records",
                   media: "FLAC",
                   artist: "London Elektricity"
                 },
                 %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
               ]
             } =
               Indexed.paginate(index, :albums,
                 order_field: :artist,
                 order_direction: :asc,
                 prefilter: {:label, "Hospital Records"}
               )
    end

    test "get_unique_values", %{index: index} do
      assert ["Hospital Records", "Liquid V Recordings"] ==
               Indexed.get_unique_values(index, :albums, :label)
    end
  end
end
