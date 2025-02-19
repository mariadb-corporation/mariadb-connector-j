<p style="text-align: center;">
	<a href="https://mariadb.com/">
		<img alt="mariadb logo" src="https://mariadb.com/kb/static/images/logo-2018-black.png">
	</a>
</p>

# MariaDB java connector

MariaDB java connector is a JDBC 4.2 compatible driver, used to connect applications developed in Java to MariaDB and
MySQL databases. MariaDB Connector/J is distributed under the LGPL license version 2.1 or later (LGPL-2.1-or-later)

Tracker link <a href="https://jira.mariadb.org/projects/CONJ/issues/">https://jira.mariadb.org/projects/CONJ/issues/</a>

## Status

[![Linux Build](https://app.travis-ci.com/mariadb-corporation/mariadb-connector-j.svg?branch=master)](https://app.travis-ci.com/github/mariadb-corporation/mariadb-connector-j)
[![Maven Central](https://img.shields.io/maven-central/v/org.mariadb.jdbc/mariadb-java-client.svg)](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client)
[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)
[![codecov][codecov-image]][codecov-url]

## Obtaining the driver

For java 8+ :

The driver (jar) can be downloaded from [mariadb connector download](https://mariadb.com/downloads/#connectors)
or maven :

```script
<dependency>
	<groupId>org.mariadb.jdbc</groupId>
	<artifactId>mariadb-java-client</artifactId>
	<version>3.5.2</version>
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
		<version>3.5.3-SNAPSHOT</version>
	</dependency>
</dependencies>
```

## Documentation

For a Getting started guide, API docs, recipes, etc. see the

* [About MariaDB connector/J](https://mariadb.com/kb/en/about-mariadb-connector-j/)
* [Install driver](https://mariadb.com/kb/en/installing-mariadb-connectorj/)
* [Changelog](/CHANGELOG.md)
* [Failover and high-availability](https://mariadb.com/kb/en/failover-and-high-availability-with-mariadb-connector-j/)

## Contributors

A big thanks to all contributors

<a href="https://github.com/mariadb-corporation/mariadb-connector-j/graphs/contributors">
	<img src="https://contrib.rocks/image?repo=mariadb-corporation/mariadb-connector-j&max=180&columns=18" />
</a>


[codecov-image]:https://codecov.io/gh/mariadb-corporation/mariadb-connector-j/branch/master/graph/badge.svg

[codecov-url]:https://codecov.io/gh/mariadb-corporation/mariadb-connector-j
