defmodule Indexed.Managed.Prepare do
  @moduledoc """
  Some tools for preparation and data normalization.
  """
  alias Indexed.{Entity, Managed}

  @doc """
  Make some automatic adjustments to the manageds list:

  * Set the `:tracked` option on the managed structs where another references it
    with a `:one` association.
  """
  @spec rewrite_managed([Managed.t()]) :: [Managed.t()]
  def rewrite_managed(manageds) do
    put_fn = fn k, fun -> &%{&1 | k => fun.(&1)} end

    map_put = fn mgs, k, fun ->
      Enum.map(mgs, put_fn.(k, &fun.(&1, mgs)))
    end

    manageds
    |> map_put.(:children, &do_rewrite_children/2)
    |> map_put.(:prefilters, &do_rewrite_prefilters/2)
    |> map_put.(:fields, &do_rewrite_fields/2)
    |> map_put.(:tracked, &do_rewrite_tracked/2)
  end

  # Normalize child association specs. Takes managed to update and list of all.
  @spec do_rewrite_children(Managed.t(), [Managed.t()]) :: Managed.children()
  defp do_rewrite_children(%{children: children, module: mod}, manageds) do
    Map.new(children, fn
      k when is_atom(k) ->
        case mod.__schema__(:association, k) do
          %{cardinality: :one} = a ->
            {k, {:one, entity_by_module(manageds, a.related), a.owner_key}}

          %{cardinality: :many} = a ->
            {k, {:many, entity_by_module(manageds, a.related), a.related_key, nil}}
        end

      {k, spec} when :many == elem(spec, 0) ->
        {k, normalize_spec(spec)}

      other ->
        other
    end)
  end

  # Auto-add prefilters needed for foreign many assocs to operate.
  # For instance, :comments would need :post_id prefilter
  # because :posts has :many comments.
  @spec do_rewrite_prefilters(Managed.t(), [Managed.t()]) :: [atom | Entity.prefilter_config()]
  defp do_rewrite_prefilters(%{prefilters: prefilters, name: name}, manageds) do
    finish = fn required ->
      Enum.uniq_by(
        Indexed.Actions.Warm.resolve_prefilters_opt(required ++ prefilters),
        &elem(&1, 0)
      )
    end

    manageds
    |> Enum.reduce([], fn %{children: children}, acc ->
      Enum.reduce(children, [], fn
        {_k, {:many, ^name, pf_key, _}}, acc2 -> [pf_key | acc2]
        _, acc2 -> acc2
      end) ++ acc
    end)
    |> case do
      [] -> prefilters
      required -> finish.(required)
    end
  end

  # If :fields is empty, use the id key or the first field given by Ecto.
  @spec do_rewrite_fields(Managed.t(), [Managed.t()]) :: [atom | Entity.field()]
  defp do_rewrite_fields(%{fields: [], id_key: id_key}, _) when is_atom(id_key),
    do: [id_key]

  defp do_rewrite_fields(%{fields: [], module: mod}, _),
    do: [hd(mod.__schema__(:fields))]

  defp do_rewrite_fields(%{fields: fields}, _), do: fields

  # Return true for tracked if another entity has a :one association to us.
  @spec do_rewrite_tracked(Managed.t(), [Managed.t()]) :: boolean
  defp do_rewrite_tracked(%{name: name}, manageds) do
    Enum.any?(manageds, fn m ->
      Enum.any?(m.children, &match?({:one, ^name, _}, elem(&1, 1)))
    end)
  end

  # Find the entity name in manageds using the given schema module.
  @spec entity_by_module([Managed.t()], module) :: atom
  defp entity_by_module(manageds, mod) do
    Enum.find_value(manageds, fn
      %{name: name, module: ^mod} -> name
      _ -> nil
    end) || raise "No entity module #{mod} in #{inspect(Enum.map(manageds, & &1.module))}"
  end

  @spec validate_before_compile!(module, module, list) :: :ok
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def validate_before_compile!(mod, repo, managed) do
    for %{children: children, module: module, name: name, subscribe: sub, unsubscribe: unsub} <-
          managed do
      inf = "in #{inspect(mod)} for #{name}"

      if (sub != nil and is_nil(unsub)) or (unsub != nil and is_nil(sub)),
        do: raise("Must have both :subscribe and :unsubscribe or neither #{inf}.")

      for {key, assoc_spec} <- children do
        related =
          case module.__schema__(:association, key) do
            %{related: r} -> r
            nil -> raise "Expected association #{key} on #{inspect(module)}."
          end

        unless Enum.find(managed, &(&1.module == related)),
          do: raise("#{inspect(related)} must be tracked #{inf}.")

        function_exported?(module, :__schema__, 1) ||
          raise "Schema module expected: #{inspect(module)} #{inf}"

        Managed.preload_fn(normalize_spec(assoc_spec), repo) ||
          raise "Invalid preload spec: #{inspect(assoc_spec)} #{inf}"
      end
    end

    :ok
  end

  # Many tuple: {:many, entity_name, prefilter_key, order_hint}
  # Optional: prefilter_key, order_hint
  @spec normalize_spec(tuple) :: tuple
  defp normalize_spec(tup) when :many == elem(tup, 0), do: expand_tuple(tup, 4)
  defp normalize_spec(tup), do: tup

  # Pad `tuple` up to `num` with `nil`.
  @spec expand_tuple(tuple, non_neg_integer) :: tuple
  defp expand_tuple(tuple, num) do
    case length(Tuple.to_list(tuple)) do
      len when len >= num ->
        tuple

      len ->
        Enum.reduce((len + 1)..num, tuple, fn _, acc ->
          Tuple.append(acc, nil)
        end)
    end
  end
end
