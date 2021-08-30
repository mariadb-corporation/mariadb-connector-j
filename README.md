<p align="center">
  <a href="http://mariadb.com/">
    <img src="https://mariadb.com/kb/static/images/logo-2018-black.png">
  </a>
</p>

# MariaDB java connector

MariaDB java connector is a JDBC 4.2 compatible driver, used to connect applications developed in Java to MariaDB and MySQL databases. MariaDB Connector/J is LGPL licensed.

Tracker link <a href="https://jira.mariadb.org/projects/CONJ/issues/">https://jira.mariadb.org/projects/CONJ/issues/</a>

## Status
[![Linux Build](https://travis-ci.com/mariadb-corporation/mariadb-connector-j.svg?branch=master)](https://travis-ci.com/mariadb-corporation/mariadb-connector-j)
[![Windows Build](https://ci.appveyor.com/api/projects/status/9o4hjqouaq2vp2v4/branch/master?svg=true)](https://ci.appveyor.com/project/mariadb/mariadb-connector-j/branch/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client)
[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)


## Obtaining the driver

For java 8 or more :
(maintenance branch for java 7 is 1.x)

The driver (jar) can be downloaded from [mariadb connector download](https://mariadb.com/products/connectors-plugins)
or maven : 
```script
<dependency>
	<groupId>org.mariadb.jdbc</groupId>
	<artifactId>mariadb-java-client</artifactId>
	<version>2.7.4</version>
</dependency>
```

Development snapshot are available on sonatype nexus repository  
```script
<repositories>
    <repository>
        <id>sonatype-nexus-snapshots</id>
        <name>Sonatype Nexus Snapshots</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>org.mariadb.jdbc</groupId>
        <artifactId>mariadb-java-client</artifactId>
        <version>3.0.2-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## Documentation

For a Getting started guide, API docs, recipes,  etc. see the 
* [About MariaDB connector/J](/documentation/about-mariadb-connector-j.creole)
* [Use MariaDB connector/j driver](/documentation/use-mariadb-connector-j-driver.creole)
* [Changelog](/CHANGELOG.md)
* [Failover and high-availability](/documentation/failover-and-high-availability-with-mariadb-connector-j.creole)


## Contributing
To get started with a development installation and learn more about contributing, please follow the instructions at our 
[Developers Guide.](/documentation/developers-guide.creole)
