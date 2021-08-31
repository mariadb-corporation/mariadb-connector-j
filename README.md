<p align="center">
  <a href="https://mariadb.com/">
    <img src="https://mariadb.com/kb/static/images/logo-2018-black.png">
  </a>
</p>

# MariaDB java connector

MariaDB java connector is a JDBC 4.2 compatible driver, used to connect applications developed in Java to MariaDB and MySQL databases. MariaDB Connector/J is LGPL licensed.

Tracker link <a href="https://jira.mariadb.org/projects/CONJ/issues/">https://jira.mariadb.org/projects/CONJ/issues/</a>

## Status
[![Linux Build](https://travis-ci.com/mariadb-corporation/mariadb-connector-j.svg?branch=master)](https://travis-ci.com/mariadb-corporation/mariadb-connector-j)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client)
[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)
[![codecov][codecov-image]][codecov-url]

## Obtaining the driver

For java 8 or more :
(maintenance branch for java 7 is 1.x)

The driver (jar) can be downloaded from [mariadb connector download](https://mariadb.com/downloads/#connectors)
or maven : 
```script
<dependency>
	<groupId>org.mariadb.jdbc</groupId>
	<artifactId>mariadb-java-client</artifactId>
	<version>2.7.4</version>
</dependency>
```

New version 3.0 is a complete rewrite with code simplification, reduced size (15%), more than 90% coverage tested, with performance gain.
But still in beta for now:
```script
<dependency>
    <groupId>org.mariadb.jdbc</groupId>
    <artifactId>mariadb-java-client</artifactId>
    <version>3.0.2-rc</version>
</dependency>
```

## Documentation

For a Getting started guide, API docs, recipes,  etc. see the 
* [About MariaDB connector/J](https://mariadb.com/kb/en/about-mariadb-connector-j/)
* [Install driver](https://mariadb.com/kb/en/installing-mariadb-connectorj/)
* [Changelog](/CHANGELOG.md)
* [Failover and high-availability](https://mariadb.com/kb/en/failover-and-high-availability-with-mariadb-connector-j/)


[codecov-image]:https://codecov.io/gh/mariadb-corporation/mariadb-connector-j/branch/master/graph/badge.svg
[codecov-url]:https://codecov.io/gh/mariadb-corporation/mariadb-connector-j
