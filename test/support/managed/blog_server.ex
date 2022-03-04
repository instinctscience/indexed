defmodule BlogServer do
  @moduledoc false
  use GenServer
  use Indexed.Managed, repo: Indexed.Test.Repo
  alias Indexed.Test.Repo

  managed :posts, Post,
    fields: [:inserted_at],
    children: [
      author: {:one, :users, :author_id},
      comments: {:many, :comments, :post_id}
    ]

  managed :comments, Comment,
    fields: [:inserted_at],
    prefilters: [:post_id],
    children: [
      author: {:one, :users, :author_id},
      post: {:one, :posts, :post_id},
      replies: {:many, :replies, :comment_id}
    ]

  managed :users, User,
    fields: [:name],
    subscribe: &Blog.subscribe_to_user/1,
    unsubscribe: &Blog.unsubscribe_from_user/1,
    children: [
      flare_pieces: {:many, :flare_pieces, :user_id}
    ]

  managed :flare_pieces, FlarePiece,
    fields: [:name],
    prefilters: [:user_id]

  # These basically exist so comments can have a :one and a :many ref.
  # When `:this_blog` is false, don't keep them in the cache.
  managed :replies, Reply,
    fields: [:id],
    children: [comment: {:one, :comments, :comment_id}]

  def call(msg), do: GenServer.call(__MODULE__, msg)
  def run(fun), do: call({:run, fun})
  def paginate(opts \\ []), do: call({:paginate, opts})

  def child_spec(opts \\ []) do
    %{
      id: __MODULE__,
      start: {GenServer, :start_link, [__MODULE__, opts, [name: __MODULE__]]}
    }
  end

  @impl GenServer
  def init(opts) do
    Process.put(:feedback_pid, opts[:feedback_pid])

    posts = Blog.all_posts()
    replies = Blog.all_replies(this_blog: true)

    {:ok,
     init_managed_state()
     |> warm(:posts, posts, author: :flare_pieces, comments: [author: :flare_pieces])
     |> warm(:replies, replies, :comment)}
  end

  @impl GenServer
  def handle_call({:create_post, author_id, content}, _from, state) do
    %Post{}
    |> Post.changeset(%{author_id: author_id, content: content})
    |> Repo.insert()
    |> case do
      {:ok, post} = ret -> {:reply, ret, manage(state, :posts, nil, post)}
      {:error, _} = ret -> {:reply, ret, state}
    end
  end

  def handle_call({:create_comment, author_id, post_id, content}, _from, state) do
    %{} = get(state, :posts, post_id)

    %Comment{}
    |> Comment.changeset(%{post_id: post_id, author_id: author_id, content: content})
    |> Repo.insert()
    |> case do
      {:ok, comment} = ret -> {:reply, ret, manage(state, :comments, nil, comment)}
      {:error, _} = ret -> {:reply, ret, state}
    end
  end

  def handle_call({:update_comment, comment_id, content}, _from, state) do
    with %{} = comment <- get(state, :comments, comment_id),
         %{valid?: true} = cs <- Comment.changeset(comment, %{content: content}),
         {:ok, new_comment} = ok <- Repo.update(cs) do
          Process.put :bb, :bb
      {:reply, ok, manage(state, :comments, comment, new_comment)}
    else
      {:error, _cs} = err -> {:reply, err, state}
      _ -> {:reply, :error, state}
    end
  end

  def handle_call({:delete_comment, comment_id}, _from, state) do
    case get(state, :comments, comment_id) do
      nil ->
        {:reply, :error, state}

      comment ->
        {:reply, Repo.delete(comment),
         manage(state, :comments, comment, nil, author: :flare_pieces)}
    end
  end

  def handle_call({:run, fun}, _from, state) do
    tools = %{
      get: &(state |> get(&1, &2) |> preload(state, &3)),
      get_records: &get_records(state, &1, nil),
      preload: &preload(&1, state, &2)
    }

    {:reply, fun.(tools), state}
  end

  def handle_call({:paginate, opts}, _from, state) do
    defaults = [
      order_by: :inserted_at,
      prepare: &preload(&1, state, opts[:preload] || [])
    ]

    opts = Keyword.merge(defaults, opts)
    page = Indexed.paginate(state.index, :posts, opts)

    {:reply, page, state}
  end

  @impl GenServer
  def handle_info({Blog, [:user, :update], %User{} = new}, state) do
    {:noreply, manage(state, :users, :update, new, :flare_pieces)}
  end

  def handle_info({Blog, [:user, :update], %FlarePiece{} = new}, state) do
    {:noreply, manage(state, :flare_pieces, :update, new)}
  end
end
