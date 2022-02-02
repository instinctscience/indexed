defmodule BlogServer do
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
      post: {:one, :posts, :post_id}
    ]

  managed :users, User,
    fields: [:name],
    # get_fn: &Blog.get_user/1,
    subscribe: &Blog.subscribe_to_user/1,
    unsubscribe: &Blog.unsubscribe_from_user/1,
    children: [
      flare_pieces: {:many, :flare_pieces, :user_id}
    ]

  managed :flare_pieces, FlarePiece,
    fields: [:name],
    # get_fn: &Blog.get_flare_piece/1,
    prefilters: [:user_id],
    subscribe: &Blog.subscribe_to_flare_piece/1,
    unsubscribe: &Blog.unsubscribe_from_flare_piece/1

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
    posts = Blog.all_posts()
    a_path = {:author, :flare_pieces}
    posts_path = [a_path, comments: [a_path]]

    {:ok,
     %{
       feedback_pid: opts[:feedback_pid],
       managed: warm(:posts, posts, posts_path)
     }}
    # |> IO.inspect(label: "INIT")
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

  def handle_call({:delete_comment, comment_id}, _from, state) do
    case get(state, :comments, comment_id) do
      nil ->
        {:reply, :error, state}

      comment ->
        ret = Repo.delete(comment)
        {:reply, ret, manage(state, :comments, comment, nil)}
    end
  end

  def handle_call({:run, fun}, _from, state) do
    tools = %{
      get: &get(state, &1, &2),
      get_records: &get_records(state, &1, nil),
      preload: &preload(&1, state, &2)
    }

    {:reply, fun.(tools), state}
  end

  def handle_call({:paginate, opts}, _from, state) do
    defaults = [
      order_by: :inserted_at,
      prepare: &resolve(&1, state.managed, preload: opts[:preload] || [])
    ]

    opts = Keyword.merge(defaults, opts)
    page = Indexed.paginate(state.managed.index, :posts, opts)

    {:reply, page, state}
  end

  @impl GenServer
  def handle_info({Blog, [:user, :update], new} = msg, state) do
    IO.inspect(new, label: "UsER")
    maybe_send(msg, state)
    %{} = orig = get(state, :users, new.id)
    {:noreply, manage(state, :users, orig, new)}
  end

  defp maybe_send(msg, %{feedback_pid: pid} = state) when is_pid(pid) do
    send(pid, msg)
    state
  end

  defp maybe_send(_, state), do: state
end
