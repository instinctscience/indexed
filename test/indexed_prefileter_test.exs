defmodule Artist do
  defstruct [:id, :label, :name]
end

defmodule IndexedPrefilterTest do
  @moduledoc "Test `:prefilter` and `:maintain_unique` options."
  use ExUnit.Case

  @artists [
    %Artist{id: 1, label: "Liquid V Recordings", name: "Calibre"},
    %Artist{id: 2, label: "Hospital Records", name: "Logistics"},
    %Artist{id: 3, label: "Hospital Records", name: "London Elektricity"},
    %Artist{id: 4, label: "Liquid V Recordings", name: "Roni Size"},
    %Artist{id: 5, label: "Hospital Records", name: "S.P.Y"}
  ]

  setup do
    [
      index:
        Indexed.warm(
          artists: [
            data: {:asc, :name, @artists},
            fields: [:name],
            prefilters: [:label]
          ]
        )
    ]
  end

  test "basic prefilter", %{index: index} do
    assert %Paginator.Page{
             entries: [
               %Artist{id: 2, label: "Hospital Records", name: "Logistics"},
               %Artist{id: 3, label: "Hospital Records", name: "London Elektricity"},
               %Artist{id: 5, label: "Hospital Records", name: "S.P.Y"}
             ],
             metadata: %Paginator.Page.Metadata{
               after: nil,
               before: nil,
               limit: 10,
               total_count: nil,
               total_count_cap_exceeded: false
             }
           } ==
             Indexed.paginate(index, :artists,
               order_field: :name,
               order_direction: :asc,
               prefilter: {:label, "Hospital Records"}
             )
  end

  test "get_unique", %{index: index} do
    assert ["Hospital Records", "Liquid V Recordings"] ==
             Indexed.get_unique(index, :artists, :label)
  end

  describe "looks good after adding a record" do
    setup %{index: index} do
      artist = %{id: 6, label: "Hospital Records", name: "Bop"}
      Indexed.set_record(index, :artists, artist)
      [artist: artist]
    end

    test "basic prefilter", %{artist: artist, index: index} do
      assert %Paginator.Page{
               entries: [
                 ^artist,
                 %Artist{id: 2, label: "Hospital Records", name: "Logistics"},
                 %Artist{id: 3, label: "Hospital Records", name: "London Elektricity"},
                 %Artist{id: 5, label: "Hospital Records", name: "S.P.Y"}
               ]
             } =
               Indexed.paginate(index, :artists,
                 order_field: :name,
                 order_direction: :asc,
                 prefilter: {:label, "Hospital Records"}
               )
    end

    test "get_unique", %{index: index} do
      assert ["Hospital Records", "Liquid V Recordings"] ==
               Indexed.get_unique(index, :artists, :label)
    end
  end
end
