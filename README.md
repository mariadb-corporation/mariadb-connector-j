# SingleStore JDBC Driver (Preview)

## Version: 0.1.0-preview

SingleStore JDBC Driver is a JDBC 4.2 compatible driver, used to connect applications developed in Java to SingleStore and MySQL databases. SingleStore JDBC Driver is LGPL licensed.

## Status
[![Linux Build](https://circleci.com/gh/memsql/S2-JDBC-Connector/tree/master.svg?branch=master)](https://circleci.com/gh/memsql/S2-JDBC-Connector)


[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)

## Obtaining the driver

The driver (jar) can be downloaded from maven :

```script
<dependency>
	<groupId>com.singlestore</groupId>
	<artifactId>singlestore-jdbc-client</artifactId>
	<version>0.1.0-preview</version>
</dependency>
```

## Usage
To get a connection using SingleStore JDBC Driver, you need a connection string of the following format:
```script
jdbc:singlestore:[replication:|loadbalance:|sequential:]//<hostDescription>[,<hostDescription>...]/[database][?<key1>=<value1>[&<key2>=<value2>]] 
```
The full list of parameters that can be used in the connection string is coming soon

Example:
```script
"jdbc:singlestore://localhost:3306/test?user=root&password=myPassword"
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

## Documentation

For a Getting started guide, API docs, recipes,  etc. see the 

[comment]: <> (TODO Add documentation links)
[comment]: <> (* [About MariaDB connector/J]&#40;https://mariadb.com/kb/en/about-mariadb-connector-j/&#41;)
[comment]: <> (* [Install driver]&#40;https://mariadb.com/kb/en/installing-mariadb-connectorj/&#41;)
* [Changelog](/CHANGELOG.md)

## User agreement

SINGLESTORE, INC. ("SINGLESTORE") AGREES TO GRANT YOU AND YOUR COMPANY ACCESS TO THIS OPEN SOURCE SOFTWARE CONNECTOR ONLY IF (A) YOU AND YOUR COMPANY REPRESENT AND WARRANT THAT YOU, ON BEHALF OF YOUR COMPANY, HAVE THE AUTHORITY TO LEGALLY BIND YOUR COMPANY AND (B) YOU, ON BEHALF OF YOUR COMPANY ACCEPT AND AGREE TO BE BOUND BY ALL OF THE OPEN SOURCE TERMS AND CONDITIONS APPLICABLE TO THIS OPEN SOURCE CONNECTOR AS SET FORTH BELOW (THIS “AGREEMENT”), WHICH SHALL BE DEFINITIVELY EVIDENCED BY ANY ONE OF THE FOLLOWING MEANS: YOU, ON BEHALF OF YOUR COMPANY, CLICKING THE “DOWNLOAD, “ACCEPTANCE” OR “CONTINUE” BUTTON, AS APPLICABLE OR COMPANY’S INSTALLATION, ACCESS OR USE OF THE OPEN SOURCE CONNECTOR AND SHALL BE EFFECTIVE ON THE EARLIER OF THE DATE ON WHICH THE DOWNLOAD, ACCESS, COPY OR INSTALL OF THE CONNECTOR OR USE ANY SERVICES (INCLUDING ANY UPDATES OR UPGRADES) PROVIDED BY SINGLESTORE.
BETA SOFTWARE CONNECTOR

Customer Understands and agrees that it is  being granted access to pre-release or “beta” versions of SingleStore’s open source software connector (“Beta Software Connector”) for the limited purposes of non-production testing and evaluation of such Beta Software Connector. Customer acknowledges that SingleStore shall have no obligation to release a generally available version of such Beta Software Connector or to provide support or warranty for such versions of the Beta Software Connector  for any production or non-evaluation use.

NOTWITHSTANDING ANYTHING TO THE CONTRARY IN ANY DOCUMENTATION,  AGREEMENT OR IN ANY ORDER DOCUMENT, SINGLESTORE WILL HAVE NO WARRANTY, INDEMNITY, SUPPORT, OR SERVICE LEVEL, OBLIGATIONS WITH
RESPECT TO THIS BETA SOFTWARE CONNECTOR (INCLUDING TOOLS AND UTILITIES).

APPLICABLE OPEN SOURCE LICENSE: GNU LESSER GENERAL PUBLIC LICENSE Version 2.1, February 1999

IF YOU OR YOUR COMPANY DO NOT AGREE TO THESE TERMS AND CONDITIONS, DO NOT DOWNLOAD, ACCESS, COPY, INSTALL OR USE THE SOFTWARE OR THE SERVICES.
