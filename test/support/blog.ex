defmodule Blog do
  @moduledoc """
  Facade for operations.
  """
  alias Indexed.Test.Repo

  @pubsub inspect(__MODULE__)

  def subscribe_to_user(id) do
    subscribe(user_subtopic(id))
  end

  def unsubscribe_from_user(id) do
    unsubscribe(user_subtopic(id))
  end

  def user_subtopic(id), do: "user-#{id}"

  def subscribe(topic) do
    Phoenix.PubSub.subscribe(@pubsub, topic)
  end

  def unsubscribe(topic) do
    Phoenix.PubSub.unsubscribe(@pubsub, topic)
  end

  def all_posts, do: Repo.all(Post)
  def all_comments, do: Repo.all(Comment)
  def all_users, do: Repo.all(User)

  def get_post(id), do: Repo.get(Post, id)
  def get_user(id), do: Repo.get(User, id)

  def create_post(author_id, content) do
    %Post{}
    |> Post.changeset(%{author_id: author_id, content: content})
    |> Repo.insert()
  end

  def create_comment(author_id, post_id, content) do
    %Comment{}
    |> Comment.changeset(%{post_id: post_id, author_id: author_id, content: content})
    |> Repo.insert()
  end

  def create_user(name) do
    %User{} |> User.changeset(%{name: name}) |> Repo.insert()
  end
end
