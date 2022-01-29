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

    [bs: start_supervised!(BlogServer.child_spec())]
  end

  test "basic", %{bs: bs} do
  end
end
