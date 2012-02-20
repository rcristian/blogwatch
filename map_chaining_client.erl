#!/usr/bin/env escript

-module(map_chaining_client).

main([Blogger, Topic, "-riakcp", RiakCPath, RiakCDepsPath]) ->
    load_libraries(RiakCPath, RiakCDepsPath),
    RPid = connect_to_riak(),
    {ok,[{_,ScanKeys}]} = scan_keys(RPid, list_to_binary(Blogger)),
    {ok,[{_,Scans}]} = scan_objects(RPid, ScanKeys),
    FilteredScans = filtered_scans(Scans, list_to_binary(Topic)),
    NoOfBlogs = blogs_count(FilteredScans),
    io:format("No. of blogs matching criteria: ~p~n", [NoOfBlogs]).

load_libraries(RiakCPath, RiakCDepsPath) ->
    code:add_patha(RiakCPath),
    code:add_patha(RiakCDepsPath).

connect_to_riak() ->
    {ok, RPid} = riakc_pb_socket:start_link("127.0.0.1", 8097),
    RPid.

% get the scan keys associated to a blogger
scan_keys(RPid, Blogger) ->
    ScanKeysForBlogger = fun(BloggerObject, undefined, none) ->
        binary_to_term(riak_object:get_value(BloggerObject))
    end,
    MapperFunBloggers = {map, {qfun, ScanKeysForBlogger}, none, true},
    riakc_pb_socket:mapred(RPid, [{<<"bloggers">>, Blogger}], [MapperFunBloggers]).

% fetch scan objects given a list of scan keys
scan_objects(RPid, ScanKeys) ->
    ScanMapper = fun(ScanObject, undefined, none) ->
        [binary_to_term(riak_object:get_value(ScanObject))]
    end,
    MapperFun = {map, {qfun, ScanMapper}, none, true},
    riakc_pb_socket:mapred(RPid, [ {<<"scans">>, SK} || SK <- ScanKeys ], [MapperFun]).

% filter scans based on topic
filtered_scans(Scans, Topic) ->
    FilterByTopic = fun(ScanEntry) ->
        case ScanEntry of
            {_Blogger, Topic, _BloggingSite, _BlogTitle} -> true;
            _ -> false
        end
    end,
    lists:map(
        fun(ScanEntry) -> lists:filter(FilterByTopic, ScanEntry) end,
        Scans
    ).

% sum up the number of blogs
blogs_count(FilteredScans) ->
    lists:foldl(fun(BlogsPerScan, Sum) -> Sum + length(BlogsPerScan) end, 0, FilteredScans).


