CQL Cassandra driver
--------------------

This library uses the [cql](http://hackage.haskell.org/package/cql) library
which implements Cassandra's CQL protocol and complements it with the
neccessary I/O operations. The feature-set includes:

**Node discovery**. The driver discovers nodes automatically from a small
set of bootstrap nodes.

**Customisable load-balancing policies**. In addition to pre-built LB
policies such as round-robin, users of this library can provide their
own policies if desired.

**Support for connection streams**. Requests can be multiplexed over a
few connections.
