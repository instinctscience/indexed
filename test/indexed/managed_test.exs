defmodule Indexed.ManagedTest do
  use Indexed.TestCase
  alias Indexed.Test.Repo

  setup do
    start_supervised!({Phoenix.PubSub, name: Blog})
    :ok
  end

  defp preload, do: [author: :flare_pieces, comments: [author: :flare_pieces]]
  defp state(bs_pid), do: :sys.get_state(bs_pid)
  defp tracking(bs_pid, name), do: Map.fetch!(state(bs_pid).tracking, name)
  defp record(name, id, pl), do: BlogServer.run(& &1.get.(name, id, pl))
  defp records(name), do: BlogServer.run(& &1.get_records.(name))
  defp paginate, do: BlogServer.paginate(preload: preload())
  defp entries, do: paginate().entries

  defp basic_setup do
    {:ok, %{id: bob_id}} = Blog.create_user("bob", ["pin"])
    {:ok, %{id: jill_id}} = Blog.create_user("jill", ["hat", "mitten"])
    {:ok, %{id: lee_id}} = Blog.create_user("lee", ["wig"])

    Repo.insert!(%Post{
      author_id: bob_id,
      content: "Hello World",
      comments: [
        %Comment{author_id: bob_id, content: "hi"},
        %Comment{author_id: bob_id, content: "ho"}
      ]
    })

    Repo.insert!(%Post{
      author_id: bob_id,
      content: "My post is the best.",
      comments: [
        %Comment{author_id: jill_id, content: "wow"},
        %Comment{author_id: lee_id, content: "woah"}
      ]
    })

    bs = start_supervised!(BlogServer.child_spec(feedback_pid: self()))

    assert_receive [:subscribe, "user-1"]
    assert_receive [:subscribe, "user-2"]
    assert_receive [:subscribe, "user-3"]

    %{bs_pid: bs}
  end

  test "basic" do
    %{bs_pid: bs_pid} = basic_setup()

    bob = Blog.get_user("bob")
    {:ok, _} = Blog.update_user(bob, %{name: "fred"})

    assert [
             %{
               content: "My post is the best.",
               author: %{name: "fred", flare_pieces: [%{id: pin_id, name: "pin"}]} = bob,
               comments: [
                 %{
                   content: "woah",
                   author: %{id: lee_id, name: "lee", flare_pieces: [%{name: "wig"} = flare]}
                 },
                 %{
                   id: comment_id,
                   content: "wow",
                   author: %{id: _jill_id, name: "jill", flare_pieces: [_, _]}
                 }
               ]
             },
             %{
               content: "Hello World",
               author: %{name: "fred"},
               comments: [%{id: comment2_id, content: "ho"}, %{content: "hi"}]
             }
           ] = entries()

    assert [%{name: "fred"}, %{name: "jill"}, %{name: "lee"}] = records(:users)
    assert %{1 => 4, 2 => 1, 3 => 1} = tracking(bs_pid, :users)

    {:ok, _} = Blog.delete_comment(comment_id)

    assert_receive [:unsubscribe, "user-2"]
    assert [%{name: "fred"}, %{name: "lee"}] = records(:users)
    assert %{1 => 4, 3 => 1} == tracking(bs_pid, :users)

    BlogServer.paginate(preload: [:comments]).entries
    |> IO.inspect(label: "hay")

    assert [%{comments: [%{content: "woah"}]}, %{comments: [_, _]}] = entries()

    refute Enum.any?(records(:flare_pieces), &(&1.name in ~w(hat mitten)))
    refute Enum.any?(records(:users), &(&1.name == "jill"))
    refute Enum.any?(records(:comments), &(&1.content == "wow"))

    {:ok, _} = Blog.update_flare(flare, %{name: "tupay"})

    assert %{name: "lee", flare_pieces: [%{name: "tupay"}]} =
             record(:users, lee_id, :flare_pieces)

    {:ok, _} =
      Blog.update_user(bob, %{
        name: "bob",
        flare_pieces: [%{id: pin_id, name: "sticker"}, %{name: "cap"}]
      })

    assert %{name: "bob", flare_pieces: [%{name: "cap"}, %{name: "sticker"}]} =
             record(:users, bob.id, :flare_pieces)

    assert %{content: "ho", author: %{name: "bob"}, post: %{content: "Hello World"}} =
             record(:comments, comment2_id, [:author, :post])
  end

  @tag :skip
  test "only :one assoc updated" do
    {:ok, bob} = Blog.create_user("bob", ["pin"])

    Repo.insert!(%Post{author_id: bob.id, content: "Hello World"})

    start_supervised!(BlogServer.child_spec(feedback_pid: self()))

    {:ok, _} = Blog.update_user(bob, %{name: "not bob"})
  end
end
