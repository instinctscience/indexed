# Indexed

Indexed is a tool for managing records in ETS.

A record is a map with an id (perhaps an Ecto.Schema struct). An ETS
table stores all such records of a given entity, keyed by id.

Configure and warm your cache with some data and get an `%Indexed{}` in
return. Pass this struct into `Indexed` functions to get, update, and paginate
records. Remember to do this from the same process which warmed the cache as
the ETS tables are protected.

## Prefilters and How Data is Indexed

A *prefilter* is applied to a field, and it partitions the data into groups
where each holds all records where the field has one particular value. For
each such grouping, indexes for sorting on each configured field under
`:fields`, ascending and descending are managed. Implicitly, the prefilter
`nil` is included where all records in the collection are included, so this
means "no prefilter". (Specify this one only if options are needed.)

Unique values for certain fields under prefilters can be tracked. An
ascending list of these values or a map where values are occurrence counts
are available with `Indexed.get_uniques_list/4` and
`Indexed.get_uniques_map/4`.

* Automatically, every prefiltered field has its unique values tracked under
  the `nil` prefilter.
* Any prefilter can list additional fields to track under its
  `:maintain_unique` option.

## Pagination

Calling `Indexed.paginate/3` returns a `%Paginator.Page{}` as defined in the
cursor-based pagination library,
[`paginator`](https://github.com/duffelhq/paginator/). The idea is that
server-side solutions are able to switch between using `paginator` to access
the database and `indexed` for fast, in-memory data, without any changes
being required on the client.

See `Indexed.Actions.Paginate` for more details.

## Managed

`Indexed.Managed` is a tool on top of the core Indexed functionality to allow a
GenServer to more easily track a set of associated records, discretely. The
`managed` macro declares an entity type and its children. Then, `manage/5` will
recursively update the cache, traveling down the hierarchy of children. While
other components of the library do not assume `Ecto.Schema` modules are being
indexed, Managed does. Subscribing and unsubscribing to record updates by ID can
be done automatically. Check the module documentation for more info.

## Installation

```elixir
def deps do
  [
    {:indexed, "~> 0.0.1"}
  ]
end
```

## Examples

```elixir
defmodule Car do
  defstruct [:id, :make]
end

cars = [
  %Car{id: 1, make: "Lamborghini"},
  %Car{id: 2, make: "Mazda"}
]

# Sidenote: for date fields, instead of an atom (`:make`) use a tuple with the
# sort option like `{:updated_at, sort: :date_time}`.
index =
  Indexed.warm(
    cars: [fields: [:make], data: cars]
  )

%Car{id: 1, make: "Lamborghini"} = car = Indexed.get(index, :cars, 1)

Indexed.set_record(index, :cars, %{car | make: "Lambo"})

%Car{id: 1, make: "Lambo"} = Indexed.get(index, :cars, 1)

Indexed.set_record(index, :cars, %Car{id: 3, make: "Tesla"})

%Car{id: 3, make: "Tesla"} = Indexed.get(index, :cars, 3)

assert [%Car{make: "Lambo"}, %Car{make: "Mazda"}, %Car{make: "Tesla"}] =
          Indexed.get_records(index, :cars, :make, :asc)

# Next, let's look at the paginator capability...

after_cursor = "g3QAAAACZAACaWRhAmQABG1ha2VtAAAABU1hemRh"

%Paginator.Page{
  entries: [
    %Car{id: 3, make: "Tesla"},
    %Car{id: 2, make: "Mazda"}
  ],
  metadata: %Paginator.Page.Metadata{
    after: ^after_cursor,
    before: nil,
    limit: 2,
    total_count: nil,
    total_count_cap_exceeded: false
  }
} = Indexed.paginate(index, :cars, limit: 2, order_field: :make, order_direction: :desc)

%Paginator.Page{
  entries: [
    %Car{id: 1, make: "Lambo"}
  ],
  metadata: %Paginator.Page.Metadata{
    after: nil,
    before: "g3QAAAACZAACaWRhAWQABG1ha2VtAAAABUxhbWJv",
    limit: 2,
    total_count: nil,
    total_count_cap_exceeded: false
  }
} =
  Indexed.paginate(index, :cars,
    after: after_cursor,
    limit: 2,
    total_count: nil,
    order_field: :make,
    order_direction: :desc
  )
```
