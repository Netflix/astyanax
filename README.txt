Astyanax is a high level Java client for Apache Cassandra.
Apache Cassandra is a highly available column oriented database: http://cassandra.apache.org
Astyanax was the son of Hector in greek mythology. 
http://en.wikipedia.org/wiki/Astyanax
http://en.wikipedia.org/wiki/Cassandra

Astyanax is currently in use at Netflix. Issues generally are fixed as quickly as possbile and releases done frequently.

Some features provided by this client:

 o high level, simple object oriented interface to cassandra
 o failover behavior on the client side
 o Connection pool abstraction.  Implementation of a round robin connection pool.
 o Monitoring abstraction to get event notification from the connection pool 
 o complete encapsulation of the underlying Thrift API and structs
 o automatic retry of downed hosts
 o automatic discovery of additional hosts in the cluster
 o suspension of hosts for a short period of time after several timeouts
 o annotations to simplify use of composite columns

Detailed documentation of Astyanax features and usage can be found on the wiki: 
https://github.com/Netflix/astyanax/wiki

The work was initially inspired by https://github.com/hector-client/hector.

