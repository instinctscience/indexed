defmodule BlogServer do
  use GenServer
  use Indexed.Managed, repo: Indexed.Test.Repo

  managed :comments, Comment,
    fields: [:inserted_at],
    children: [
      author: {:get, :users, :author_id},
      post: {:get, :posts, :post_id}
    ]

  managed :posts, Post,
    fields: [:inserted_at],
    children: [
      author: {:get, :users, :author_id},
      comments: {:get_records, :comments, nil, :updated_at}
    ]

  managed :users, User,
    fields: [:name],
    get_fn: &Blog.get_user/1,
    subscribe: &Blog.subscribe_to_user/1,
    unsubscribe: &Blog.unsubscribe_from_user/1

  # def child_spec(id, opts \\ []) do
  def child_spec do
    %{
      id: __MODULE__,
      start: {GenServer, :start_link, [__MODULE__, nil]}
    }
  end

  # def start_link do
  #   GenServer.start_link(__MODULE__, nil)
  # end

  @impl GenServer
  def init(nil) do
    posts = Blog.all_posts()
    posts_path = [:author, comments: :author]
    {:ok, warm(:posts, posts, posts_path)}
  end
end
