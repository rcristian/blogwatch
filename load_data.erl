#!/usr/bin/env escript

main([Filename, "-riakcp", RiakCPath, RiakCDepsPath]) ->
    load_libraries(RiakCPath, RiakCDepsPath),
    RPid = connect_to_riak(),
    {ok, Data} = file:read_file(Filename),
    Lines = tl(re:split(Data, "\r?\n", [{return, binary},trim])),
    lists:foreach(fun(L) -> LS = re:split(L, ","), insert_blog_record(RPid, LS) end, Lines).

load_libraries(RiakCPath, RiakCDepsPath) ->
    code:add_patha(RiakCPath),
    code:add_patha(RiakCDepsPath).

connect_to_riak() ->
    {ok, RPid} = riakc_pb_socket:start_link("127.0.0.1", 8097),
    RPid.

insert_blog_record(RPid, Line) ->
    % Note we won't insert all values into the DB - like blog text - for sake of brevity

    % date scanned,blogger,blog topic,blog title,blogging site,blog contents
    [DateScanned, Blogger, BlogTopic, BlogTitle, BloggingSite, _BlogContents] = Line,
    ScanKey = create_update_scan(RPid, DateScanned, Blogger, BlogTopic, BlogTitle, BloggingSite),
    create_update_blogger(RPid, Blogger, ScanKey),
    create_update_topic(RPid, BlogTopic, ScanKey),
    create_update_bloggingsite(RPid, BloggingSite, ScanKey).

create_update_scan(RPid, DateScanned, Blogger, BlogTopic, BlogTitle, BloggingSite) ->
    ScanEntry = {Blogger, BlogTopic, BloggingSite, BlogTitle},
    ok = update_object_value_list(RPid, <<"scans">>, DateScanned, ScanEntry),
    DateScanned.

create_update_blogger(RPid, Blogger, ScanID) ->
    ok = update_object_value_list(RPid, <<"bloggers">>, Blogger, ScanID),
    Blogger.
    
create_update_topic(RPid, BlogTopic, ScanID) ->
    ok = update_object_value_list(RPid, <<"topics">>, BlogTopic, ScanID),
    BlogTopic.

create_update_bloggingsite(RPid, BloggingSite, ScanID) ->
    ok = update_object_value_list(RPid, <<"sites">>, BloggingSite, ScanID),
    BloggingSite.


update_object_value_list(RPid, Bucket, ObjectKey, Value) ->
    FetchedObject = riakc_pb_socket:get(RPid, Bucket, ObjectKey),
    ObjectToUpdate = case FetchedObject of
        {ok, Object} -> 
            OldVal = binary_to_term(riakc_obj:get_value(Object)),
            case lists:member(Value, OldVal) of
                false -> riakc_obj:update_value(Object, lists:append(OldVal, [Value]));
                true -> Object
            end;
        {error, notfound} -> riakc_obj:new(Bucket, ObjectKey, [Value])
    end,
    riakc_pb_socket:put(RPid, ObjectToUpdate).



    

