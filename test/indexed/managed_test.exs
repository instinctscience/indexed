defmodule Indexed.ManagedTest do
  use Indexed.TestCase
  alias Indexed.Test.Repo

  setup do
    start_supervised!({Phoenix.PubSub, name: Blog})

    # {:ok, %{id: bob_id}} = Blog.create_user("bob")
    # {:ok, %{id: jill_id}} = Blog.create_user("jill")
    # {:ok, %{id: lee_id}} = Blog.create_user("lee")

    # {:ok, %{id: pid}} = Blog.create_post(bob_id, "Hello World")
    # {:ok, _} = Blog.create_comment(bob_id, pid, "hi")
    # {:ok, _} = Blog.create_comment(bob_id, pid, "ho")
    # # {:ok, _} = Blog.create_comment(bob_id, pid, "silver")

    # {:ok, %{id: pid}} = Blog.create_post(bob_id, "My post is the best.")
    # {:ok, _} = Blog.create_comment(jill_id, pid, "wow")
    # {:ok, _} = Blog.create_comment(lee_id, pid, "woah")
    # # {:ok, _} = Blog.create_comment(bob_id, pid, "woo")

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

    [bs_pid: start_supervised!(BlogServer.child_spec(feedback_pid: self()))]
  end

  test "basic", %{bs_pid: bs_pid} do
    # %{id: bob_id} = bob = Blog.get_user("bob")
    # {:ok, _} = Blog.update_user(bob, %{name: "fred"})

    # assert_receive {Blog, [:user, :update], %{id: ^bob_id}}

    # assert %{name: "fred"} = BlogServer.run(& &1.get.(:users, bob_id))

    # :sys.get_state(bs_pid) |> IO.inspect(label: "stat")
    # BlogServer.run(& &1.get_records.(:users)) |> IO.inspect(label: "users")

    assert %{
             entries: [
               %{
                 content: "My post is the best.",
                 author: %{name: "bob", flare_pieces: [%{name: "pin"}]},
                 comments: [
                   %{content: "woah", author: %{name: "lee", flare_pieces: [%{name: "wig"}]}},
                   %{
                     id: comment_id,
                     content: "wow",
                     author: %{id: _jill_id, name: "jill", flare_pieces: [_, _]}
                   }
                 ]
               },
               %{
                 content: "Hello World",
                 author: %{name: "bob"},
                 comments: [%{content: "ho"}, %{content: "hi"}]
               }
             ]
           } =
             BlogServer.paginate(
               preload: [author: :flare_pieces, comments: [author: :flare_pieces]]
             )

    #  |> IO.inspect(label: "PAGGA")

    #  :sys.get_state(bs_pid)
    #  |> IO.inspect(label: "endstate")

    state = fn -> :sys.get_state(bs_pid) end
    tracking = fn -> state.().managed.tracking end

    assert %{users: %{1 => 4, 2 => 1, 3 => 1}} = tracking.()

    IO.inspect(label: "redeh")

    {:ok, _} = Blog.delete_comment(comment_id)

    assert %{users: %{1 => 4, 3 => 1}} = tracking.()

    # todo - subscription handling on many-type assocs

    # {:ok, _} = Blog.update_user(Blog.get_user("jill"), %{name: "jessica"})
  end
end
