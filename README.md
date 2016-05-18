# mariadb-connector-j
<p align="center">
  <a href="http://gulpjs.com">
    <img height="129" width="413" src="http://badges.mariadb.org/logo/Mariadb-seal-shaded-browntext.png">
  </a>
</p>

MariaDB Connector/J is used to connect applications developed in Java to MariaDB and MySQL databases. MariaDB Connector/J is LGPL licensed.

Tracker link <a href="https://mariadb.atlassian.net/projects/CONJ/issues/">https://mariadb.atlassian.net/projects/CONJ/issues/</a>

## Status
[![Build Status](https://travis-ci.org/MariaDB/mariadb-connector-j.svg?branch=master)](https://travis-ci.org/MariaDB/mariadb-connector-j)
[![Maven central](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client /badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client)
[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)

## Obtaining the driver
The driver (jar) can be downloaded from [mariadb connector download](https://mariadb.com/products/connectors-plugins)

or maven : 

```script
<dependency>
	<groupId>org.mariadb.jdbc</groupId>
	<artifactId>mariadb-java-client</artifactId>
	<version>1.4.5</version>
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
        <version>1.5.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## Documentation

For a Getting started guide, API docs, recipes,  etc. see the 
* [About MariaDB connector/J](/documentation/About-MariaDB-Connector-J.md)
* [Use MariaDB connector/j driver](/documentation/Use-MariaDB-Connector-j-driver.md)
* [Failover and high-availability](/documentation/Failover-and-high-availability.md)


## Contributing
To get started with a development installation and learn more about contributing, please follow the instructions at our 
[Developers Guide.](/documentation/Developers-Guide.md)

