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

## Obtaining the driver
The driver (jar) can be downloaded from [mariadb connector download](https://mariadb.com/products/connectors-plugins)

or maven : 

```script
<dependency>
	<groupId>org.mariadb.jdbc</groupId>
	<artifactId>mariadb-java-client</artifactId>
	<version>1.2.2</version>
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
        <version>1.3.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## Documentation

For a Getting started guide, API docs, recipes,  etc. see the [documentation page](https://mariadb.com/kb/en/mariadb/about-the-mariadb-java-client/)!
