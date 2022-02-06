defmodule Indexed.ManagedTest do
  use Indexed.TestCase
  alias Indexed.Test.Repo

  setup do
    start_supervised!({Phoenix.PubSub, name: Blog})

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

    [bs_pid: bs]
  end

  test "basic", %{bs_pid: bs_pid} do
    preload = [author: :flare_pieces, comments: [author: :flare_pieces]]
    state = fn -> :sys.get_state(bs_pid) end
    tracking = fn name -> Map.fetch!(state.().tracking, name) end
    records = fn name -> BlogServer.run(& &1.get_records.(name)) end
    paginate = fn -> BlogServer.paginate(preload: preload) end
    entries = fn -> paginate.().entries end

    %{id: bob_id} = bob = Blog.get_user("bob")
    {:ok, _} = Blog.update_user(bob, %{name: "fred"})

    assert_receive {:got, {Blog, [:user, :update], %{id: ^bob_id}}}

    assert [
             %{
               content: "My post is the best.",
               author: %{name: "fred", flare_pieces: [%{name: "pin"}]},
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
               author: %{name: "fred"},
               comments: [%{content: "ho"}, %{content: "hi"}]
             }
           ] = entries.()

    assert %{1 => 4, 2 => 1, 3 => 1} = tracking.(:users)

    {:ok, _} = Blog.delete_comment(comment_id)

    assert_receive [:unsubscribe, "user-2"]

    assert %{1 => 4, 3 => 1} == tracking.(:users)

    assert [%{comments: [%{content: "woah"}]}, %{comments: [_, _]}] = entries.()

    refute Enum.any?(records.(:flare_pieces), &(&1.name in ~w(hat mitten)))
    refute Enum.any?(records.(:users), &(&1.name == "jill"))
    refute Enum.any?(records.(:comments), &(&1.content == "wow"))

    # todo - subscription handling on many-type assocs

    # {:ok, _} = Blog.update_user(Blog.get_user("jill"), %{name: "jessica"})
  end
end
