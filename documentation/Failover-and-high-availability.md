
> **This guide will teach you:**
> * The load balancing and high availability concepts in Mariadb java connector
> * the different options

Failover and high availability were introduced in 1.2.0.


# Load balancing and failover distinction  
Failover occurs when a  connection to a primary database server fails and  the connector will open up a connection to another database server.<br/>
For example, server A has the current connection. After a failure (server crash, network down …) the connection will switch to another server (B).

Load balancing allows load (read and write) to be distributed over multiple servers.
<br/>
# Replication cluster type
In MariaDB (and MySQL) replication, there are 2 different replication roles:
* Master role: Database server that permits read and write operations
* Slave role: Database server that permits only read operations

This document describes configuration and implementation for 3 types of clusters:
* Multi-Master replication cluster. All hosts have a master replication role. (example : Galera)
* Master/slaves cluster: one host has the master replication role with multiple hosts in slave replication role.
* Hybrid cluster: multiple hosts in master replication role with multiple hosts in slave replication role. 

# Load balancing implementation
## Random picking
When initializing a connection or after a failed connection, the connector will attempt to connect to a host with a certain role (slave/master). 
The connection is selected randomly among the valid hosts. Thereafter, all statements will run on that database server until the connection will be closed (or fails).

The load-balancing will includes a pooling mechanism. 
Example: when creating a pool of 60 connections, each one will use a random host. With 3 master hosts, the pool will have about 20 connections to each host.

## Master/slave distributed load

For a cluster composed of masters and slaves on connection initialization, there will be 2 underlying connections: one with a master host, another with a slave host. Only one connection is used at a time. <br/>
For a cluster composed of master hosts only, each connection has only one underlying connection. <br/>
The load will be distributed due to the random distribution of connections..<br/>

## Master/slave connection selection

It’s the application that has to decide to use master or slave connection (the master connection is set by default).<br/>
Switching the type of connection is done by using JDBC [connection.setReadOnly(boolean readOnly)](http://docs.oracle.com/javase/7/docs/api/java/sql/Connection.html#setReadOnly(boolean)) method. Setting read-only to true will use the slave connection, false, the master connection.<br/>

Example in standard java:
``` java
connection = DriverManager.getConnection("jdbc:mysql:replication://master1,slave1/test");
stmt = connection.createStatement();
stmt.execute("SELECT 1"); // will execute query on the underlying master1 connection
connection.setReadOnly(true);
stmt.execute("SELECT 1"); // will execute query on the underlying slave1 connection
```

Some frameworks render this kind of operation easier, as for example Spring [@transactionnal](http://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/transaction/annotation/Transactional.html#readOnly--) readOnly parameter (since spring 3.0.1).
In this example, setting readOnly to false will call the connection.setReadOnly(false) and therefore use the master connection.
``` java
@Autowired
private EntityManager em;

@Transactional(readOnly = false, propagation = Propagation.REQUIRED)
public void createContacts() {
  Contact contact1 = new Contact();
  contact1.setGender("M");
  contact1.setName("JIM");
  em.persist(contact1);
}
```

Generated Spring Data repository objects use the same logic: the find* method will use the slave connection, other use master connection without having to explicitly set that for each method.

On a cluster with master hosts only, the use of connection.setReadOnly(true) does not change the connection, but if the database version is 10.0.0 or higher, the session is set to readOnly if option assureReadOnly is set to true, which means that any write query will throw an exception.

#Failover behaviour
##Basic failover 
When no failover/high availability parameter is set, the failover support is basic. Before executing a query, if the connection with the host is discarded, the connection will be reinitialized if parameter “autoReconnect” is set to true.

##Standard failover
When a failover /high availability parameter is set.Check the [configuration](#configuration) section for an overview on how to set the parameters.

There can be multiple fail causes. When a failure occurs many things will be done: 
* The fail host address will be put on a blacklist (shared by JVM). This host will not be used for the amount of time defined by the “loadBalanceBlacklistTimeout” parameter (default to 50 seconds). The only time a blacklisted address can be used is if all host of the same type (master/slave) are blacklisted.
* The connector will check the connection (with the mysql [ping protocol](https://dev.mysql.com/doc/internals/en/com-ping.html)). If the connection is back, is not read-only, and is in a transaction, the transaction will be rollbacked (there is no way to know if the last query has been received by the server and executed). 
* If the failure relates to a slave connection
  *     If the master connection is still active, the master connection will be used immediately. 
        The query that was read-only will be relaunched and the connector will not throw any exception. 
        A "failover" thread will be launched to attempt to reconnect a slave host. 
        (if the query was a prepared query, this query will be re-prepared before execution)
  *     If the master connection is not active, the driver will attempt to create a new master or slave connection with a [connection loop](#connection-loop).
        if any connection is found, the query will be relaunched, if not, an SQLException with sqlState like “08XXX” will be thrown.  
* If the failure relates to a master connection, the driver will attempt to create a new master connection with a [connection loop](#connection-loop), so the connection object will be immediately reusable.<br>
  *       on failure, an SQLException with be thrown with SQLState "08XXX". If using a pool, this connection will be discarded.
  *       on success,    
    *       if possible query will be relaunched without throwing error (if was using a slave connection, or was a SELECT query not in a transaction for example).
    *       if not possible, an SQLException with be thrown with SQLState "25S03".  
* When throwing an SQLException with SQLState "08XXX", the connection will be marked as closed. 
* A “failover” thread will be launched to attempt to reconnect failing connection if connection is not closed.  

It’s up to the application to take measures to handle SQLException. See details in [application concerns](#application-concerns).

#Connection loop
When initializing a connection or after a failure, the driver will launch a connection loop the only case when this connection loop will not be executed is when the failure occurred on a slave with an active master.
This connection loop will try to connect to a valid host until finding a new connection or until the number of connections exceed the parameter “retriesAllDown” value (default to 120).

This loop will attempt to connect sequentially to hosts in the following order:

For a master connection : 
* random connect to master host not blacklisted
* random connect to master blacklisted

For a slave connection : 
* random connect to slave host not blacklisted
* random connect to master host not blacklisted (if no active master connection)
* random connect to slave blacklisted
* random connect to master host blacklisted (if no active master connection)
The sequence stops as soon as all the underlying needed connections are found. Every time an attempt fails, the host will be blacklisted. 
If after an entire loop a master connection is missing, the connection will be marked as closed. 

#Additional threads

##Failover reconnection threads
A thread pool is created in case of a master/slave cluster, the size is defined according to the number of connection.  
After a failure on a slave connection, readonly operations are temporary executed on the master connection. Some “failover threads” will try to reconnect the failed underlying connections.
When a new slave connection is retrieved, this one will be immediately used if connection was still in read-only mode.<br> 
More details in [Failover loop threads](./Failover_loop).


##Connection validation thread
An additional thread is created when setting the option "validConnectionTimeout".
This thread will very that connections are all active. 
This is normally done by pool that call [Connection.isValid()](https://docs.oracle.com/javase/7/docs/api/java/sql/Connection.html#isValid(int)).
 
#Application concerns
When a failover happen a SQLException with sqlState like "08XXX" or "25S03" may be thrown.

Here are the different connection error codes:

|Code       |Condition |
|-----------|:----------|
|08000      | connection exception|
|08001      |SQL client unable to establish SQL connection|
|08002      |connection name in use|
|08003      |connection does not exist|
|08004      |SQL server rejected SQL connection|
|08006      |connection failure|
|08007      |transaction resolution unknown|
|-----------|:----------|
|25S03      |invalid transaction state-transaction is rolled back|

A connection pool will detect connection error in SQLException (SQLState begin with "08"), and this connection will be discarded from pool.<br/>

When a failover occur the connector cannot know if the last request has been received by the database server and executed. Applications may have failover design to handle these particular cases: 
If the application was in autoCommit mode (not recommended), the last query may have been executed and committed. The application will have no possibility to know that but the application will be functional.
If not in autoCommit mode, the query has been launched in a transaction that will not be committed. Depending of what caused the exception, the host may have the connection open on his side during a certain amount of time. Take care of [transaction isolation](https://mariadb.com/kb/en/mariadb/set-transaction/) level that may lock too much rows.
  


#Configuration

(See [About MariaDB java connector](./About-MariaDB-Connector-J.md) for all connection parameters)
JDBC connection string format is :
```script
jdbc:(mysql|mariadb):[replication:|failover:|loadbalance:|aurora:]//<hostDescription>[,<hostDescription>...]/[database][?<key1>=<value1>[&<key2>=<value2>]...]
```

The standard option "connectTimeout" defined the socket connection timeout. by default, these option is set to 0 (no timeout).<br>
Since there are many servers, setting this option to a small amount of time make sense.<br>
During the [connection loop phase](#connection-loop), the driver will try to connect to server sequentially until the creation of an active connection.
Set this option to a small value (like 2000ms - to be set according to your environment) will permit to reject faulty server quickly.   


##Failover / high availability parameters

Each parameter corresponds to a specific use case:

|Failover option|Description|
|-----------|:----------|
| **failover** | High availability (random picking connection initialisation) with failover support for master replication cluster (exemple Galera). <br/>* Since 1.2.0*|
| **sequential** |Failover support for master replication cluster (for example Galera) **without** High availability. <br/>the host will be connected in the order in which they were declared.<br/><br/>Example when using the jdbc url string "jdbc:mysql:replication:host1,host2,host3/test" : <br/>When connecting, the driver will always try first host1, and if not available host2 and following. After a host fail, the driver will reconnect according to this order.<br/>*Since 1.3.0*|
| **replication** | High availability (random picking connection initialisation) with failover support for master/slaves replication cluster (one or multiple master).<br/>* Since 1.2.0*|
| **aurora** | High availability (random picking connection initialisation) with failover support for Amazon Aurora replication cluster.<br/>* Since 1.2.0*|


##Failover / high availability options

|Option|Description|
|-----------|:----------|
|autoReconnect|With basic failover only, if true, will attempt to recreate connection after a failover.<br/>*Default is false. Since 1.1.7*|
|retriesAllDown|When searching a valid host, maximum number of connection attempts before throwing an exception.<br/>*Default: 120. Since 1.2.0|
|failoverLoopRetries|When searching silently for a valid host, maximum number of connection attempts.<br/>This differ from "retriesAllDown" parameter, because this silent search is for example used after a disconnection of a slave connection when using the master connection.<br/>*Default: 120. Since 1.2.0*|
|validConnectionTimeout|With multiple hosts, after this time in seconds has elapsed it’s verified that the connections haven’t been lost.<br/>When 0, no verification will be done.<br/>*Default:120 seconds. Since 1.2.0*|
|loadBalanceBlacklistTimeout|When a connection fails, this host will be blacklisted during the "loadBalanceBlacklistTimeout" amount of time.<br/>When connecting to a host, the driver will try to connect to a host in the list of not blacklisted hosts and after that only on blacklisted ones if none has been found before that.<br/>This blacklist is shared inside the classloader.<br/>*Default: 50 seconds. Since 1.2.0*|
|assureReadOnly|If true, in high availability, and switching to a read-only host, assure that this host is in read-only mode by setting session read-only.<br/>alias "readOnlyPropagatesToServer" worked to for compatibility<br/>*Default to false.<br/> Since 1.3.0*|


#Specifics for Amazon Aurora

Amazon Aurora is a Master/Slaves cluster composed of one master instance with a maximum of 15 slave instances. Amazon Aurora includes automatic promotion of a slave instance in case of the master instance failing. The MariaDB connector/J implementation for Aurora is specific to handle this automatic failover.<br/>

To permit development/integration on a single-node cluster, only one host can be defined.  
In this case, the driver behaves as for the configuration **failover**. 
    

##Aurora failover implementation
Aurora failover management steps : 
* Instance A is in write replication mode, instance B and C are in read replication mode.
* Instance A fails.
* Aurora detects A failure, and promote instance B in write mode. Instance C will change his master to use B. 
* Cluster end-point will change to instance B end-point.
* Instance A will recover and be in read replication mode.  

##Aurora configuration

###Aurora endpoints

Every instance has a specific endpoint, ie an URL that identify the host. Those endpoints look like “xxxx.yyyy.us-east-1.rds.amazonaws.com”.

There is another endpoint named “cluster endpoint” which is assigned to the current master instance and will change when a new master is promoted.

This cluster endpoint must never be used in the URL connection string for 2 reasons:
* When a failover occurs, a new master is promoted. The cluster endpoint change to this new master is not immediately effective. The connector doesn’t use this cluster endpoint. Instead it points to the new master immediately.
* More important, an instance will not be used, and load will be poorly distributed. \\Example : \\Normally JDBC url string must be like :\\##jdbc:mysql:aurora://A.XX.com,B.XX.com,C.XX.com/db##\\if the master endpoint is used : \\##jdbc:mysql:aurora://master.XX.com,B.XX.com,C.XX.com/db## \\If B become master, A will not be used at all and C will receive all the read queries.

###JDBC connection string

The implementation is activated by specifying the “aurora” failover parameter.
```script
jdbc:(mysql|mariadb):aurora://[instanceEndPoint[:port]][,instanceEndPoint[:port]...]/[database][?<key1>=<value1>[&<key2>=<value2>]...]
```

Host declaration use instance endpoint (never cluster endpoint). 

The replication role of each instance must not be defined for Aurora, because the role of each instance changes over time. The driver will check the instance role after connection initialisation.

Example of connection string
 `jdbc:mysql:aurora://host1.xxxx.us-east-1.rds.amazonaws.com,host2.xxxx.us-east-1.rds.amazonaws.com/db`

Another difference is the option "socketTimeout" that defaults to 10 seconds, meaning that - if not changed - queries exceeding 10 seconds will throw exceptions.  

##Aurora connection loop

When searching for the master instance and connect to a slave instance, the connection order will be: 
* Every Aurora instance knows the hostname of the current master. If the host has been described using their instance endpoint, that will permit to know the master instance and connect directly to it.  
* If this isn’t the current master (because using IP, or possible after a failover between step 2 and 3), the loop will connect randomly the other not blacklisted instance (minus the current slave instance) 
* Connect randomly to a blacklisted instance.

When searching for a slave instance, the loop will connection order will be:
* random not blacklisted instances (excluding the current host if connected)
* random blacklisted instances 
The loop will retry until the connections are found or parameter “retriesAllDown” is exceeded.

##Aurora master verification
Without any query during the time defined by the parameter validConnectionTimeout (default to 120s) and if not set to 0, a verification will be done that the replication role of the underlying connections haven’t changed.

##Aurora connection validation thread
Aurora as a specific [connection validation thread](#connection-validation-thread) implementation. 
Since role of each instance can change over time, this will validate that connection are active AND role have not changed.

 