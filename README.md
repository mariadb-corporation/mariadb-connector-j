# SingleStore JDBC Driver

## Version: 1.2.12-beta

SingleStore JDBC Driver is a JDBC 4.2 compatible driver, used to connect applications developed in Java to SingleStore and MySQL databases. SingleStore JDBC Driver is LGPL licensed.

[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)

## Obtaining the driver

The driver (jar) can be downloaded from maven :

```script
<dependency>
	<groupId>com.singlestore</groupId>
	<artifactId>singlestore-jdbc-client</artifactId>
	<version>1.2.12-beta</version>
</dependency>
```

## Usage
To get a connection using SingleStore JDBC Driver, you need a connection string of the following format:
```script
jdbc:singlestore:[loadbalance:|sequential:]//<hostDescription>[,<hostDescription>...]/[database][?<key1>=<value1>[&<key2>=<value2>]] 
```

Example:
```script
"jdbc:singlestore://localhost:3306/test?user=root&password=myPassword"
```

### Proxy support

The driver supports connecting through a SOCKS proxy:

- `socksProxyHost` / `socksProxyPort` (port defaults to `1080` when host is set and port is omitted)

Example:
```
jdbc:singlestore://localhost:3306/test?user=root&password=myPassword&socksProxyHost=127.0.0.1&socksProxyPort=1080
```

You can then get a connection by using the [Driver manager](https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html) class:
```script
Connection connection = DriverManager.getConnection("jdbc:singlestore://localhost:3306/test?user=root&password=myPassword");
Statement stmt = connection.createStatement();

ResultSet rs = stmt.executeQuery("SELECT NOW()");
rs.next();
System.out.println(rs.getTimestamp(1));
```

Another way to get a connection with SingleStore JDBC Driver is to use a connection pool.
* `SingleStorePoolDataSource` is an implementation that maintains a pool of connections. When a new connection is requested, one is borrowed from the pool.
* `SingleStoreDataSource` is a basic implementation that just returns a new connection each time the `getConnection()` method is called.

Example:
```script
SingleStorePoolDataSource pool = new SingleStorePoolDataSource("jdbc:singlestore://server/db?user=myUser&maxPoolSize=10");

    try (Connection connection = pool.getConnection()) {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT NOW()");
            rs.next();
            System.out.println(rs.getTimestamp(1));
        }
    }
```

## Extended types support

SingleStore supports extended types metadata starting from database version 8.7.1. Enable the `enable_extended_types_metadata` session engine variable to use extended types in SingleStore. By default, support for extended types is disabled for backward compatibility and the database uses the standard data type format. 

To correctly handle the extended type metadata with the SingleStore JDBC driver, set the `enableExtendedDataTypes` parameter to `true`. This parameter is only supported in SingleStore JDBC driver version 1.2.6 and later. By default, `enableExtendedDataTypes` is set to `false`.
**Note**: Based on the value of the `enableExtendedDataTypes` parameter, the driver automatically configures the value of `enable_extended_types_metadata` engine variable while initializing the connection, which overrides the current value of this engine variable.


Extended types:

| Data Type                                                                                         | Updates                                                                                     |
|----------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| [Vector](https://docs.singlestore.com/cloud/reference/sql-reference/data-types/vector-type/) | Added `com.singlestore.jdbc.type.Vector` type to read/write Vector data.<br/>Updated metadata methods. |
| [BSON](https://docs.singlestore.com/cloud/reference/sql-reference/data-types/bson-type/)     | Updated metadata methods.                                                                             |

Example 
```script
Connection connection = DriverManager.getConnection("jdbc:singlestore://localhost:3306/test?user=root&password=myPassword&enableExtendedDataTypes=true&vectorTypeOutputFormat=JSON");
Statement stmt = connection.createStatement();

stmt.execute("CREATE TABLE extended_types (a1 VECTOR(3, I16), a2 BSON)");

PreparedStatement pstmt = connection.prepareStatement("INSERT INTO extended_types VALUES(?, '{\"a1\":\"test\"}' :> BSON)");

pstmt.setObject(1, Vector.ofInt16Values(new short[]{1, 2, 3}));
pstmt.execute();

ResultSet rs = stmt.executeQuery("SELECT * FROM extended_types");
rs.next();
Vector vector = (Vector) rs.getObject(1);
SingleStoreBlob bson = (SingleStoreBlob) rs.getObject(2);
System.out.println(vector);

```

## Building from source

### Requirements
* [Maven](https://maven.apache.org/download.cgi)
* JDK 11

Clone the respository:
```script
git clone https://github.com/memsql/S2-JDBC-Connector.git
```

Execute the following from the repository root to build SingleStore JDBC Driver from source:
```script
mvn -Dmaven.test.skip -Dmaven.javadoc.skip package
```

This generates a `singlestore-jdbc-client-<version>.jar` file in `target/` directory.
Install this file to a directory in your `CLASSPATH` to use the driver.

## Documentation

For the documentation and a getting started guide refer to
[SingleStore Documentation](https://docs.singlestore.com/managed-service/en/developer-resources/connect-with-application-development-tools/connect-with-java-jdbc/the-singlestore-jdbc-driver.html)

## Releasing

To cut a new release, bump the version and push a `v*` tag. See [RELEASING.md](RELEASING.md) for the full process.

## User agreement

SINGLESTORE, INC. ("SINGLESTORE") AGREES TO GRANT YOU AND YOUR COMPANY ACCESS TO THIS OPEN SOURCE SOFTWARE CONNECTOR ONLY IF (A) YOU AND YOUR COMPANY REPRESENT AND WARRANT THAT YOU, ON BEHALF OF YOUR COMPANY, HAVE THE AUTHORITY TO LEGALLY BIND YOUR COMPANY AND (B) YOU, ON BEHALF OF YOUR COMPANY ACCEPT AND AGREE TO BE BOUND BY ALL OF THE OPEN SOURCE TERMS AND CONDITIONS APPLICABLE TO THIS OPEN SOURCE CONNECTOR AS SET FORTH BELOW (THIS “AGREEMENT”), WHICH SHALL BE DEFINITIVELY EVIDENCED BY ANY ONE OF THE FOLLOWING MEANS: YOU, ON BEHALF OF YOUR COMPANY, CLICKING THE “DOWNLOAD, “ACCEPTANCE” OR “CONTINUE” BUTTON, AS APPLICABLE OR COMPANY’S INSTALLATION, ACCESS OR USE OF THE OPEN SOURCE CONNECTOR AND SHALL BE EFFECTIVE ON THE EARLIER OF THE DATE ON WHICH THE DOWNLOAD, ACCESS, COPY OR INSTALL OF THE CONNECTOR OR USE ANY SERVICES (INCLUDING ANY UPDATES OR UPGRADES) PROVIDED BY SINGLESTORE.

NOTWITHSTANDING ANYTHING TO THE CONTRARY IN ANY DOCUMENTATION,  AGREEMENT OR IN ANY ORDER DOCUMENT, SINGLESTORE WILL HAVE NO WARRANTY, INDEMNITY, SUPPORT, OR SERVICE LEVEL, OBLIGATIONS WITH
RESPECT TO THIS SOFTWARE CONNECTOR (INCLUDING TOOLS AND UTILITIES).

APPLICABLE OPEN SOURCE LICENSE: GNU LESSER GENERAL PUBLIC LICENSE Version 2.1, February 1999

IF YOU OR YOUR COMPANY DO NOT AGREE TO THESE TERMS AND CONDITIONS, DO NOT DOWNLOAD, ACCESS, COPY, INSTALL OR USE THE SOFTWARE OR THE SERVICES.
