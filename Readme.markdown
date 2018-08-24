Astyanax
========

Deprecation
-----------
Astyanax has been [retired](https://medium.com/netflix-techblog/astyanax-retiring-an-old-friend-6cca1de9ac4) and is no 
longer under active development but may receive dependency updates to ease migration away from Astyanax.

In place of Astyanax consider using [DataStax Java Driver for Apache Cassandra](https://github.com/datastax/java-driver) 
which is under active development and encapsulates a lot of lessons learned from Astyanax. Since the DataStax driver 
supports only CQL protocol (because Apache Cassandra 4.x will drop the Thrift protocol), you’ll have to update all of 
your Thrift-based queries to CQL queries.
                                                                                           
You have the option to continue accessing legacy column families via the CQL “with compact storage” option. However in 
most (all?) use cases the legacy compact storage option actually takes more space than the new default CQL storage 
option, so you’re really better off also migrating your data to the new default storage option.

This version of Astyanax shades its dependency on cassandra-all so you can optionally select any version 
of cassandra-unit you like for unit/integration testing with different versions of Cassandra. When upgrading to this 
version of Astyanax you may need to:
* Explicitly add any cassandra-all transitive dependencies you previously silently depended on via Astayanax transitives.
* If you were using internal features of Astyanax that (unintentionally) publicly exposed cassandra-all classes, 
you must either:
    * Option A (best): Migrate away from Astyanax to [DataStax Java Driver for Apache Cassandra](https://github.com/datastax/java-driver).
    * Option B (second-best): Use only astyanax-core public interfaces (none of them expose cassandra-all classes).
    * Option C: Switch your objects from "org.apache.cassandra" to the shaded "com.netflix.astyanax.shaded.org.apache.cassandra"
     package Astyanax now depends on.

Overview
--------
Astyanax is a high level Java client for [Apache Cassandra](http://cassandra.apache.org).
Apache Cassandra is a highly available column oriented database.

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

IntelliJ: currently (June 2018) IntelliJ has a bug which renders an "unfound" error for packages relocated via another 
module in the same project (e.g. shaded classes from astyanax-cassandra-all-shaded). 
This affects *only* the Astyanax project itself within IntelliJ; Astyanax still builds perfectly fine via Gradle. 
Astyanax users are unaffected. For more details see:

* [https://github.com/johnrengelman/shadow/issues/264](https://github.com/johnrengelman/shadow/issues/264) 
* [https://youtrack.jetbrains.com/issue/IDEA-163411](https://youtrack.jetbrains.com/issue/IDEA-163411)

Artifacts
-------------------------------

Astyanax jars are published to Maven Central.  As of astyanax 1.56.27 the project has been split into multiple sub project, each of which needs to be pulled in separately.

Required artifacts

|GroupID/Org|ArtifactID/Name|Desc|
| --------- | ------------- |----|
|com.netflix.astyanax|astyanax-thrift or astyanax-cql|Choose Thrift or CQL protocol. Note Cassandra 4.x+ drops support for Thrift protocol.|

Transitive artifacts (dependencies automatically added via a required artifact)

|GroupID/Org|ArtifactID/Name|Desc|
| --------- | ------------- |----|
|com.netflix.astyanax|astyanax-core|Astyanax's public interface.|
|com.netflix.astyanax|astyanax-cassandra|Cassandra-specific features shared by astyanax-thrift and astyanax-cql|
|com.netflix.astyanax|astyanax-cassandra-all-shaded|Shaded version of cassandra-all for the few classes used by astyanax-cassandra so projects are free to select arbitrary versions of cassandra-unit for unit/integration testing against newer versions of Cassandra. Hides Astyanax's dependency on cassandra-all by refactoring "org.apache.cassandra" classes to "com.netflix.astyanax.shaded.org.apache.cassandra".|

Optional artifacts

|GroupID/Org|ArtifactID/Name|Desc|
| --------- | ------------- |----|
|com.netflix.astyanax|astyanax-contrib|Optional integration with other commonly-used Netflix OSS modules.|
|com.netflix.astyanax|astyanax-queue|Queue implementation backed by Cassandra storage. Use at your own risk -- Cassandra can be a *very bad* storage engine for queues. If you insist on using Cassandra for queues, set a very short TTL and use TimeWindowCompactionStrategy (TWCS).|
|com.netflix.astyanax|astyanax-entity-mapper||
|com.netflix.astyanax|astyanax-recipes|Optional implementations of some common patterns. Use at your own risk; some of these are popular but not well-suite for Cassandra for many use cases (looking at you, AllRowsReader).|


Ancient History
---------------
[Astyanax](http://en.wikipedia.org/wiki/Astyanax) was the son of [Hector](http://en.wikipedia.org/wiki/Hector) who was [Cassandra's](http://en.wikipedia.org/wiki/Cassandra) brother in greek mythology. 


Modern History
----------------
This work was initially inspired by [Hector](https://github.com/hector-client/hector).

