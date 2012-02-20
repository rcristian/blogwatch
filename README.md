## Description

Code to illustrate map-reduce - chained 'map' - operations with Riak's Erlang client.

## Running

You'll need Erlang, Riak running on local machine and the Riak client library.

First populate the DB with some data by running:

    $ ./load_data.erl blog-data.csv -riakcp *path-to-riak-erlang-client*/ebin *path-to-riak-erlang-client*/deps/protobuffs/ebin

Then run the map-reduce operation to get some aggregated data from the DB:

    $ ./map_chaining.erl robert78 best-pencil-ever-made -riakcp *path-to-riak-erlang-client*/ebin *path-to-riak-erlang-client*/deps/protobuffs/ebin

