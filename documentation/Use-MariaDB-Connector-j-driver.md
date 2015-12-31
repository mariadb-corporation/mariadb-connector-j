
# Using the driver

The following subsections show the formatting of JDBC connection strings for
MariaDB, MySQL  database servers.  Additionally, sample code is provided that
demonstrates how to connect to one of these servers and create a table.


## Driver Manager

Applications designed to use the driver manager to locate the entry point need
no further configuration, MariaDB Connector/J will
automatically be loaded and used in the way any previous MySQL driver would
have been.

## Driver Class 

Please note that the driver class provided by MariaDB Connector/J **is not
`com.mysql.jdbc.Driver` but `org.mariadb.jdbc.Driver`**!


## Connection strings

Format of the JDBC connection string is 
```script
jdbc:(mysql|mariadb):[replication:|failover:]//<hostDescription>[,<hostDescription>...]/[database][?<key1>=<value1>[&<key2>=<value2>]] 
```

 HostDescription:
```script
<host>[:<portnumber>]  or address=(host=<host>)[(port=<portnumber>)][(type=(master|slave))]
```

Host must be a DNS name or IP address. In case of ipv6 and simple host
description, the IP address must be written inside brackets.  The default port
is ##3306##.  The default type is ##master##. If ##replication## failover is
set, by default the first host is master, and the others are slaves.

Examples : 
* localhost:3306
* [2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306
* somehost.com:3306
* address=(host=localhost)(port=3306)(type=master)


### Failover parameters

Failover was introduced in Connector/J 1.2.0.
See [failover and high availability documentation](./Failover-and-high-availability.md) for more informations.


| Failover option | Description| 
| ------------ |:----------------| 
| **failover** | High availability (random picking connection initialisation) with failover support for master replication cluster (for example Galera). <br/>*Since 1.2.0*|
| **sequential** |Failover support for master replication cluster (for example Galera) **without** High availability. <br/>the host will be connected in the order in which they were declared.<br/><br/>Example when using the jdbc url string "jdbc:mysql:replication:host1,host2,host3/test" : <br/>When connecting, the driver will always try first host1, and if not available host2 and following. After a host fail, the driver will reconnect according to this order.<br/>*Since 1.3.0*|
| **replication** | High availability (random picking connection initialisation) with failover support for master/slaves replication cluster (one or multiple master).<br/>*Since 1.2.0*|
| **aurora** | High availability (random picking connection initialisation) with failover support for Amazon Aurora replication cluster.<br/>*Since 1.2.0*|




### Optional URL parameters

General remark: Unknown options accepted and are silently ignored.

Following options are currently supported.

| Option | Description| 
| ------------ |:----------------| 
|<a name="user">user</a>|Database user name. <br/>*since 1.0.0*|
|<a name="password">password</a>|Password of database user.<br/>*since 1.0.0*|
|<a name="useFractionalSeconds">useFractionalSeconds</a>| Correctly handle subsecond precision in timestamps (feature available with MariaDB 5.3 and later).<br/>May confuse 3rd party components (Hibernated).<br/>*Default: true. Since 1.0.0*|
|<a name="allowMultiQueries">allowMultiQueries</a>| Allows multiple statements in single executeQuery.<br/>example:<br/>`insert into ab (i) values (1); insert into ab (i) values (2);`<br/>will be rewritten <br/>`insert into ab (i) values (1), (2);`<br/>*Default: false. Since 1.0.0*|
|<a name="dumpQueriesOnException">dumpQueriesOnException</a>|If set to 'true', exception thrown during query execution contain query string.<br/>*Default: false.Since 1.1.0*|
|<a name="useCompression">useCompression</a>|allow compression in MySQL Protocol.<br/>*Default: false. Since 1.0.0*|
|<a name="useSsl">useSsl</a>|Force SSL on connection.<br/>Alias useSSL works too for mysql compatibility<br/>*Default: false. Since 1.1.0*|
|<a name="trustServerCertificate">trustServerCertificate</a>|When using SSL, do not check server's certificate.<br/>*Default: false. Since 1.1.1|
|<a name="serverSslCert">serverSslCert</a>|Server's certificatem in DER form, or server's CA certificate.<br/>Can be used in one of 3 forms : <br/>* sslServerCert=/path/to/cert.pem (full path to certificate)<br/>* sslServerCert=classpath:relative/cert.pem (relative to current classpath)<br/>* or as verbatim DER-encoded certificate string "------BEGING CERTIFICATE-----" .<br/>*Since 1.1.3*|
|<a name="socketFactory">socketFactory</a>| to use custom socket factory, set it to full name of the class that implements javax.net.SocketFactory.<br/>*Since 1.0.0*|
|<a name="tcpNoDelay">tcpNoDelay</a>|Sets corresponding option on the connection socket.<br/>*Default: true. Since 1.0.0*|
|<a name="tcpKeepAlive">tcpKeepAlive</a>|Sets corresponding option on the connection socket.<br/>*Since 1.0.0*|
|<a name="tcpAbortiveClose">tcpAbortiveClose</a>|Sets corresponding option on the connection socket.<br/>*Since 1.1.1*|
|<a name="tcpRcvBuf">tcpRcvBuf</a>| set buffer size for TCP buffer (SO_RCVBUF).<br/>*Since 1.0.0*|
|<a name="tcpSndBuf">tcpSndBuf</a>| set buffer size for TCP buffer (SO_SNDBUF).<br/>*Since 1.0.0*|
|<a name="pipe">pipe</a>| On Windows, specify  named pipe name to connect to mysqld.exe.<br/>*Since 1.1.3*|
|<a name="tinyInt1isBit">tinyInt1isBit</a>| Datatype  mapping flag, handle MySQL Tiny as BIT(boolean).<br/>Default: true.<br/>*Since 1.0.0*|
|<a name="yearIsDateType">yearIsDateType</a>|Year is date type, rather than numerical.<br/><br/>Default: true.<br/>*Since 1.0.0*|
|<a name="sessionVariables">sessionVariables</a>|<var>=<value> pairs separated by comma, mysql session variables, set upon establishing successfull connection.<br/>*Since 1.1.0*|
|<a name="localSocket">localSocket</a>|Allows to connect to database via Unix domain socket, if server allows it. <br/>The value is the path of Unix domain socket (i.e "socket" database parameter : select @@socket) .<br/>*Since 1.1.4*|
|<a name="sharedMemory">sharedMemory</a>|Allowed to connect database via shared memory, if server allows it. <br/>The value is base name of the shared memory.<br/>*Since 1.1.4*|
|<a name="localSocketAddress">localSocketAddress</a>|Hostname or IP address to bind the connection socket to a local (UNIX domain) socket.<br/>*Since 1.1.7*|
|<a name="socketTimeout">socketTimeout</a>|Defined the network socket timeout (SO_TIMEOUT) in milliseconds. <br/>Default: 0 milliseconds(0 disable this timeout). Since 1.1.7*|
|<a name="interactiveClient">interactiveClient</a>|Session timeout is defined by the wait_timeout server variable. Setting interactiveClient to true will tell server to use the interactive_timeout server variable.<br/>*Default: false. Since 1.1.7*|
|<a name="useOldAliasMetadataBehavior">useOldAliasMetadataBehavior</a>|Metadata ResultSetMetaData.getTableName() return the physical table name.  "useOldAliasMetadataBehavior" permit to activate the legacy code that send the table alias if set. <br/>*Default: false. Since 1.1.9*|
|<a name="createDatabaseIfNotExist">createDatabaseIfNotExist</a>|the specified database in url will be created if nonexistent.<br/>Default: false. Since 1.1.7*|
|<a name="serverTimezone">serverTimezone</a>|Defined the server time zone.<br/>to use only if jre server as a different time implementation of the server.<br/>(best to have the same server time zone when possible). <br/>Since 1.1.7*|
|<a name="rewriteBatchedStatements">rewriteBatchedStatements</a>| rewrite batchedStatement to have only one server call.<br/>*Default: false. Since 1.1.8*|
|<a name="useServerPrepStmts">useServerPrepStmts</a>| if true, preparedStatement will be prepared on server side. If not, Prepared statements (parameter substitution) is handled by the driver, on the client side.<br/>*Default: true. Since 1.3.0*| 
|<a name="connectTimeout">connectTimeout</a>| The connect  the timeout value, in milliseconds, or zero for no timeout.<br/>*Default: 0. Since 1.1.8*|
|<a name="alwaysAutoGeneratedKeys">alwaysAutoGeneratedKeys</a>|JDBC permit to retrieve previous insert id's by the [Statement.getGeneratedKeys()](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#getGeneratedKeys--) command.<br/> By default id's are not retrieved. When executing query, additional parameters Statement.NO_GENERATED_KEYS or Statement.RETURN_GENERATED_KEYS indicate if id's must be retrieved, like in command [Statement.executeUpdate(String sql, int autoGeneratedKeys)](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#executeUpdate-java.lang.String-int-).<br/><br/> When option alwaysAutoGeneratedKeys is set to true, insert id's will be retrieved even if no autoGeneratedKeys parameter or [Statement.NO_GENERATED_KEYS](https://docs.oracle.com/javase/8/docs/api/java/sql/Statement.html#NO_GENERATED_KEYS) is set.<br/> *Default: false. Since 1.3.0*|


### Failover/High availability URL parameters


| Option | Description| 
| ------------ |:----------------| 
|<a name="autoReconnect">autoReconnect</a>|With basic failover: if true, will attempt to recreate connection after a failover. <br/><br/>With standard failover: if true, will attempt to recreate connection even if there is a temporary solution (like using a master connection temporary until reconnect to a slave connection) <br/><br/>Default is false.<br/><br/>since 1.1.7|
|<a name="retriesAllDown">retriesAllDown</a>|When searching a valid host, maximum number of connection attempts before throwing an exception.<br/><br/>Default: 120 seconds.<br/><br/>since 1.2.0|
|<a name="failoverLoopRetries">failoverLoopRetries</a>|When searching silently for a valid host, maximum number of connection attempts.<br/><br/>This differ from "retriesAllDown" parameter, because this silent search is for example used after a disconnection of a slave connection when using the master connection<br/><br/>Default: 120.<br/><br/>since 1.2.0|
|<a name="validConnectionTimeout">validConnectionTimeout</a>|With multiple hosts, after this time in seconds has elapsed it’s verified that the connections haven’t been lost.<br/><br/>When 0, no verification will be done. <br/><br/>Default:120 seconds<br/><br/>since 1.2.0|
|<a name="loadBalanceBlacklistTimeout">loadBalanceBlacklistTimeout</a>|When a connection fails, this host will be blacklisted during the "loadBalanceBlacklistTimeout" amount of time.<br/><br/>When connecting to a host, the driver will try to connect to a host in the list of not blacklisted hosts and after that only on blacklisted ones if none has been found before that.<br/><br/>This blacklist is shared inside the classloader.<br/><br/>Default: 50 seconds.<br/><br/>since 1.2.0|
|<a name="assureReadOnly">assureReadOnly</a>|If true, in high availability, and switching to a read-only host, assure that this host is in read-only mode by setting session read-only.<br/>Default to false.<br/>*Default: 50 seconds. Since 1.3.0*|
<br/>

## JDBC API Implementation Notes
### Streaming result sets
By default, `Statement.executeQuery()` will read full result set
from server before returning. With large result sets, this will require large
amounts of memory. Better behavior in this case would be reading row-by-row,
with `ResultSet.next()`, so called "streaming" feature. It is
activated using `Statement.setFetchSize(Integer.MIN_VALUE)`


### CallableStatement
Callable statement implementation won't need to access stored procedure
metadata ([[mysqlproc-table|mysql.proc]]) table if both of following are true

* CallableStatement.getMetadata() is not used
* Parameters are accessed by index, not by name

When possible, following the two rules above provides both better speed and
eliminates concerns about SELECT privileges on the
[[mysqlproc-table|mysql.proc]] table.

### Optional JDBC classes
Following optional interfaces are implemented by the
org.mariadb.jdbc.MariaDbDataSource class : javax.sql.DataSource,
javax.sql.ConnectionPoolDataSource, javax.sql.XADataSource

## Usage examples

The following code provides a basic example of how to connect to a MariaDB or
MySQL server and create a table.  

### Creating a table on a MariaDB or MySQL Server
```java
Connection  connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "username", "password");
Statement stmt = connection.createStatement();
stmt.executeUpdate("CREATE TABLE a (id int not null primary key, value varchar(20))");
stmt.close();
connection.close();
```

