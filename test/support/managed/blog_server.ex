defmodule BlogServer do
  use GenServer
  use Indexed.Managed, repo: Indexed.Test.Repo

  managed :comments, Comment,
    fields: [:inserted_at],
    prefilters: [:post_id],
    children: [
      author: {:one, :users, :author_id},
      post: {:one, :posts, :post_id}
    ]

  managed :posts, Post,
    fields: [:inserted_at],
    children: [
      author: {:one, :users, :author_id},
      comments: {:many, :comments, :post_id}
    ]

  managed :users, User,
    fields: [:name],
    get_fn: &Blog.get_user/1,
    subscribe: &Blog.subscribe_to_user/1,
    unsubscribe: &Blog.unsubscribe_from_user/1

  def child_spec(opts \\ []) do
    %{
      id: __MODULE__,
      start: {GenServer, :start_link, [__MODULE__, opts]}
    }
  end

  @impl GenServer
  def init(opts) do
    posts = Blog.all_posts()
    posts_path = [:author, comments: :author]

    {:ok,
     %{
       feedback_pid: opts[:feedback_pid],
       managed: warm(:posts, posts, posts_path)
     }}
  end

  @impl GenServer
  def handle_info({Blog, [:user, :update], user}, state) do
    IO.inspect(user, label: "UsER")
    with pid when is_pid(pid) <- state[:feedback_pid] do
      send(pid, {Blog, [:user, :update], user})
    end

    {:noreply, state}
  end
end
