#!/usr/bin/env escript

-module(map_chaining).

main([Blogger, Topic, "-riakcp", RiakCPath, RiakCDepsPath]) ->
    load_libraries(RiakCPath, RiakCDepsPath),
    RPid = connect_to_riak(),
    {ok,[{_,[NoOfBlogs]}]} = get_number_of_blogs(RPid, list_to_binary(Blogger), list_to_binary(Topic)),
    io:format("No. of blogs matching criteria: ~p~n", [NoOfBlogs]).

load_libraries(RiakCPath, RiakCDepsPath) ->
    code:add_patha(RiakCPath),
    code:add_patha(RiakCDepsPath).

connect_to_riak() ->
    {ok, RPid} = riakc_pb_socket:start_link("127.0.0.1", 8097),
    RPid.

get_number_of_blogs(RPid, Blogger, Topic) ->
    % function to get the scan objects associated with a blogger
    ScanKeysForBlogger = fun(BloggerObject, undefined, none) ->
        ScanKeys = binary_to_term(riak_object:get_value(BloggerObject)),
        % provide input for the next map opperation that will look into the 'scans' bucket
        [ [<<"scans">>, SK] || SK <- ScanKeys ]
    end,
    MapperFunBloggers = {map, {qfun, ScanKeysForBlogger}, none, false},

    % function to filter blogs that are bound to a blogger / blog topic
    FilterTopic = fun(ScanObject, _ScanData, {BloggerToFilter, BlogTopic}) ->
        BlogsList = binary_to_term(riak_object:get_value(ScanObject)),
        FilteredBlogsList = lists:filter(
            fun(Blg) -> case Blg of
                            {BloggerToFilter, BlogTopic, _, _} -> true;
                            _ -> false
                       end
            end, BlogsList
        ),
        [length(FilteredBlogsList)]
    end,
    MapperFunScans = {map, {qfun, FilterTopic}, {Blogger, Topic}, false},

    CountBlogsFun = fun(BlogsCount, none) -> 
        [lists:foldl(fun(NoBlogs, Sum) -> NoBlogs + Sum end, 0, BlogsCount)]
    end,
    ReduceFunCount = {reduce, {qfun, CountBlogsFun}, none, true},

    riakc_pb_socket:mapred(RPid, [{<<"bloggers">>, Blogger}], [MapperFunBloggers, MapperFunScans, ReduceFunCount]).



