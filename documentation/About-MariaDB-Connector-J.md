# About MariaDB java connector

MariaDB Connector/J is used to connect applications developed in Java to
MariaDB and MySQL databases using the standard JDBC API. The library is LGPL
licensed.


## Introduction
MariaDB Connector/J is a Type 4 JDBC driver.  It was developed specifically as
a lightweight JDBC connector for use with MySQL and MariaDB database servers.
It's originally based on the Drizzle JDBC code, and with a lot of additions and
bug fixes.

## Obtaining the driver
The driver source code can be downloaded from:
https://downloads.mariadb.org/connector-java/

Pre-built .jar files can be downloaded from:
https://code.mariadb.com/connectors/java/


## Installing the driver
Installation of the client library is very simple, the jar file should be saved
in an appropriate place for your application and the classpath of your
application altered to include MariaDB Connector/J rather than your current
connector.

Using maven : 
```script
<dependency>
    <groupId>org.mariadb.jdbc</groupId>
    <artifactId>mariadb-java-client</artifactId>
    <version>xxx</version>
</dependency>
```

## Requirements

* Java 7 or 8 (Last compatible version with java 6 is [1.1.9](https://downloads.mariadb.org/connector-java/1.1.9/))
* com.sun.JNA is used by some library functions and a jar is available at
  https://github.com/twall/jna
** only needed when connecting to the server with unix sockets or windows
   shared memory
* A MariaDB or MySQL Server 
* maven (only if you want build from source)

## Source code

The source code is available on GitHub:
https://github.com/MariaDB/mariadb-connector-j and the most recent development
version can be obtained using the following command:

```script
git clone https://github.com/MariaDB/mariadb-connector-j.git
```

## License

GNU Lesser General Public License as published by the Free Software Foundation;
either version 2.1 of the License, or (at your option) any later version.


## Building and testing the driver

The section deals with building the connector from source and testing it. If
you have downloaded a ready built connector, in a jar file, then this section
may be skipped.

MariaDB Client Library for Java Applications uses maven for build. You first
need to ensure you have both java and maven installed on your server before you
can build the driver.

To run the unit test, you'll need a MariaDB or MySQL server running on
localhost (on default TCP port 3306) and a database called 'test', and user
'root' with empty password

```script
git clone https://github.com/MariaDB/mariadb-connector-j.git #  Or, unpack the source distribution tarball
cd mariadb-connector-j
# For the unit test run, start local mysqld mysqld, 
# ensure that user root with empty password can login
mvn package
# If you want to build without running unit  tests, use
# mvn -Dmaven.test.skip=true package
```

After that, you should have JDBC jar mariadb-java-client-x.y.z.jar in the
'target' subdirectory

