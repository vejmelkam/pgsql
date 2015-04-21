%% @doc PostgreSQL connection (high level functions).
-module(pgsql_manager).
-vsn("1").
-behaviour(gen_server).
-include("pgsql_internal.hrl").

-export([
         % pool manager API
         start_link/4,
         stop/0,

         % dedicated connection API
         % the module supports the user obtaining dedicated connections
         % which are then used with pgsql_connection directly (if left open,
         % connections are closed when server terminates).
         open/0,
         close/1,
         
         % simple_query & extended_query API
         simple_query/1,
         simple_query/2,
         simple_query/3,

         extended_query/2,
         extended_query/3,
         extended_query/4,

         % gen_server API
         init/1,
         handle_call/3,
         handle_cast/2,
         code_change/3,
         handle_info/2,
         terminate/2
        ]).


start_link(Dbase,User,Pass,NumConns) ->
  case gen_server:start_link({local, pgsql_manager}, pgsql_manager, {Dbase,User,Pass,NumConns}, []) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} ->
      case get_dbase() of
        Dbase ->
          % don't start a new server but extend the current connection pool by the number of connections <NumConns>
          gen_server:call(Pid, {extend_conn_pool, NumConns}),
          {ok, Pid}
          % a different dbase will intentionally crash the routine
      end
    % other error cases will [intentionally] crash this routine
  end.


stop() ->
  gen_server:cast(pgsql_manager, stop).


open() ->
  gen_server:call(pgsql_manager, open).


close(C) ->
  gen_server:call(pgsql_manager, {close, C}).

get_dbase() ->
  gen_server:call(pgsql_manager, get_dbase).


%%--------------------------------------------------------------------
%% @doc Perform a simple query via the pool.
%%
-spec simple_query(iodata()) ->
  pgsql_connection:result_tuple() | {error, any()} | [pgsql_connection:result_tuple() | {error, any()}].
simple_query(Query) ->
  pgsql_connection:simple_query(Query, round_robin_get_next()).


-spec simple_query(iodata(), pgsql_connection:query_options()) ->
  pgsql_connection:result_tuple() | {error, any()} | [pgsql_connection:result_tuple() | {error, any()}].
simple_query(Query, QueryOptions) ->
  pgsql_connection:simple_query(Query, QueryOptions, round_robin_get_next()).


%%--------------------------------------------------------------------
%% @doc Perform a simple query with query options and a timeout.
%%
-spec simple_query(iodata(), pgsql_connection:query_options(), timeout()) ->
  pgsql_connection:result_tuple() | {error, any()} | [pgsql_connection:result_tuple() | {error, any()}].
simple_query(Query, QueryOptions, Timeout) ->
  pgsql_connection:simple_query(Query, QueryOptions, Timeout, round_robin_get_next()).


%%--------------------------------------------------------------------
%% @doc Perform an extended query.
%%
-spec extended_query(iodata(), [any()]) -> pgsql_connection:result_tuple() | {error, any()}.
extended_query(Query, Parameters) ->
  pgsql_connection:extended_query(Query, Parameters, round_robin_get_next()).

-spec extended_query(iodata(), [any()], pgsql_connection:query_options()) -> pgsql_connection:result_tuple() | {error, any()}.
extended_query(Query, Parameters, QueryOptions) ->
  pgsql_connection:extended_query(Query, Parameters, QueryOptions, round_robin_get_next()).

%% @doc Perform an extended query with query options and a timeout.
%% See discussion of simple_query/4 about timeout values.
%%
-spec extended_query(iodata(), [any()], pgsql_connection:query_options(), timeout()) -> pgsql_connection:result_tuple() | {error, any()}.
extended_query(Query, Parameters, QueryOptions, Timeout) ->
  pgsql_connection:extended_query(Query, Parameters, QueryOptions, Timeout, round_robin_get_next()).


%%--------------------------------------------------------------------
%% internal functions
%%


round_robin_get_next() ->
  gen_server:call(pgsql_manager, rr_get_next).


make_new_connections(N,{Dbase,User,Pass}) ->
  lists:map(fun (_) -> pgsql_connection:open(Dbase,User,Pass) end, lists:seq(1,N)).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%


init({Dbase,User,Pass,NumConns}) ->
  {ok, R} = pgsql_connection_sup:start_link(),
  Cs = make_new_connections(NumConns,{Dbase,User,Pass}),
  {ok, {R, Cs, [], [], {Dbase,User,Pass}}}.


handle_call(get_dbase,_From,State={_R,[],_Used,_Pool,{Db,_User,_Pass}}) ->
  {reply, Db, State};
handle_call(rr_get_next,_From,{R,[],Used,Pool,Credentials}) ->
  [Conn|N1] = lists:reverse(Used),
  {reply, Conn, {R,N1,[Conn],Pool,Credentials}};
handle_call(rr_get_next,_From,{R,[Conn|N1],Used,Pool,Credentials}) ->
  {reply, Conn, {R,N1,[Conn|Used],Pool,Credentials}};
handle_call(open,_From,{R,Nxt,Used,Pool,Credentials={Db,User,Pass}}) ->
  Conn = pgsql_connection:open(Db,User,Pass),
  {reply, Conn, {R,Nxt,Used,[Conn|Pool],Credentials}};
handle_call({close,C},_From,{R,Nxt,Used,Pool,Credentials}) ->
  pgsql_connection:close(C),
  {reply, ok, {R,Nxt,Used,lists:delete(C,Pool),Credentials}};
handle_call({extend_conn_pool, N},_From,{R,Nxt,Used,Pool,Credentials}) ->
  Cs = make_new_connections(N,Credentials),
  {reply, ok, {R,Cs++Nxt,Used,Pool,Credentials}};
handle_call(_Request,_From,State) ->
  {reply, not_understood, State}.


handle_cast(stop,State) ->
  {stop, normal, State};
handle_cast(_Cmd,State) ->
  {noreply, State}.


handle_info(_Msg,State) ->
  {noreply, State}.


code_change(_Vsn, State, _Extra) ->
    {ok, State}.

terminate(normal,{R,Nxt,Used,Pool,_}) ->
  lists:map(fun pgsql_connection:close/1, lists:flatten([Nxt,Used,Pool])),
  exit(R,kill);
terminate(_Reason,_State) ->
  ok.

