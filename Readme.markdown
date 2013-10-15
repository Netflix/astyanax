Astyanax
========
Astyanax is a high level Java client for [Apache Cassandra](http://cassandra.apache.org).
Apache Cassandra is a highly available column oriented database.

Astyanax is currently in use at [Netflix](http://movies.netflix.com). Issues generally are fixed as quickly as possible and releases done frequently.

Artifacts
-------------------------------

Astyanax jars are published to Maven Central.  As of astyanax 1.56.27 the project has been split into multiple sub project, each of which needs to be pulled in separately.

Required artifacts

|GroupID/Org|ArtifactID/Name|
| --------- | ------------- |
|com.netflix.astyanax|astyanax-core|
|com.netflix.astyanax|astyanax-thrift|
|com.netflix.astyanax|astyanax-cassandra|

Optional artifacts

|GroupID/Org|ArtifactID/Name|
| --------- | ------------- |
|com.netflix.astyanax|astyanax-queue|
|com.netflix.astyanax|astyanax-entity-mapper|
|com.netflix.astyanax|astyanax-recipes|

Features
--------
A quick overview can be found at the [Netflix Tech Blog](http://techblog.netflix.com/2012/01/announcing-astyanax.html). Some features provided by this client:

* High level, simple object oriented interface to Cassandra.
* Fail-over behavior on the client side.
* Connection pool abstraction.  Implementation of a round robin connection pool.
* Monitoring abstraction to get event notification from the connection pool. 
* Complete encapsulation of the underlying Thrift API and structs.
* Automatic retry of downed hosts.
* Automatic discovery of additional hosts in the cluster.
* Suspension of hosts for a short period of time after several timeouts.
* Annotations to simplify use of composite columns.


Documentation
-------------
Detailed documentation of Astyanax's features and usage can be found on the [wiki](https://github.com/Netflix/astyanax/wiki) and the [getting started guide](https://github.com/Netflix/astyanax/wiki/Getting-Started).


Ancient History
---------------
[Astyanax](http://en.wikipedia.org/wiki/Astyanax) was the son of [Hector](http://en.wikipedia.org/wiki/Hector) who was [Cassandra's](http://en.wikipedia.org/wiki/Cassandra) brother in greek mythology. 


Modern History
----------------
This work was initially inspired by [Hector](https://github.com/hector-client/hector).

