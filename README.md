## psql-native

This library is an implementation of the postgres client protocol version 3.0, whose support started from PostgreSQL 7.4 and later.

The library goals are:
 1. Use as low resources as possible.
 2. Minimize copying data as much as possible.
 3. Support asynchronous I/O using vibe.d sockets.

The library is still a work in progress and major re-architecturing may happen anytime.

Once the first release is done, issues and contributions are welcome.

