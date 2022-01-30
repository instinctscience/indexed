defmodule Indexed.ManagedTest do
  use Indexed.TestCase

  setup do
    {:ok, %{id: uid}} = Blog.create_user("bob")
    {:ok, %{id: pid}} = Blog.create_post(uid, "Hello World")
    {:ok, _} = Blog.create_comment(uid, pid, "hi")
    {:ok, _} = Blog.create_comment(uid, pid, "ho")
    # {:ok, _} = Blog.create_comment(uid, pid, "silver")

    {:ok, %{id: uid}} = Blog.create_user("jill")
    {:ok, %{id: pid}} = Blog.create_post(uid, "My post is the best.")
    {:ok, _} = Blog.create_comment(uid, pid, "wow")
    {:ok, _} = Blog.create_comment(uid, pid, "woah")
    # {:ok, _} = Blog.create_comment(uid, pid, "woo")

    start_supervised!({Phoenix.PubSub, name: Blog})
    [bs: start_supervised!(BlogServer.child_spec(feedback_pid: self()))]
  end

  test "basic", %{bs: bs} do
    user = Blog.get_user(1)
    Blog.update_user(user, %{name: "fred"})

    assert_receive {Blog, [:user, :update], %{id: 1, name: "fred"}}
    # Process.sleep 20
  end
end
