# Grubhub Fork
This fork contains some fixes needed for AWS Aurora Mysql in terms of connecting to a read-only cluster. Hoping this is a very temporary fork.

## Testing
Nearly all unit tests have been excluded via `build.gradle` because they require a mysql instance to connect to and one isn't accessible in our build environment.

You should still run these tests locally. Assuming you have brew installed, do the following:

```shell script
# Install mysql 5.7.x
brew install mysql@5.7
# Manually start mysql
/usr/local/opt/mysql@5.7/bin/mysql.server start
# Secure your server with a password (Gradle is configured for 'guest')
/usr/local/opt/mysql@5.7/bin/mysql_secure_installation
# Connect to mysql with your password 
/usr/local/opt/mysql@5.7/bin/mysql -uroot -pguest
# Create the database in the mysql CLI
create database testj;
exit
 
```

Now you can run  `./gradlew mysqlTest` to execute the tests that connect to mysql. A few tests may fail; use your best judgement.

<p align="center">
  <a href="http://mariadb.com/">
    <img src="https://mariadb.com/kb/static/images/logo-2018-black.png">
  </a>
</p>

# MariaDB java connector

MariaDB java connector is a JDBC 4.2 compatible driver, used to connect applications developed in Java to MariaDB and MySQL databases. MariaDB Connector/J is LGPL licensed.

Tracker link <a href="https://jira.mariadb.org/projects/CONJ/issues/">https://jira.mariadb.org/projects/CONJ/issues/</a>

## Status
[![Linux Build](https://travis-ci.org/MariaDB/mariadb-connector-j.svg?branch=master)](https://travis-ci.org/MariaDB/mariadb-connector-j)
[![Windows Build](https://ci.appveyor.com/api/projects/status/7hpe3wmbu57r8noa/branch/master?svg=true)](https://ci.appveyor.com/project/rusher/mariadb-connector-j/branch/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.mariadb.jdbc/mariadb-java-client)
[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/be7f4c89d63e496d824e8f365478e8c8)](https://www.codacy.com/app/diego-dupin/mariadb-connector-j?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MariaDB/mariadb-connector-j&amp;utm_campaign=Badge_Grade)

## Obtaining the driver

| Java version | current version |
|:------------:|:-------------------------:|
| 6 | 1.7.4 |
| 7 | 1.7.4 |
| 8+ | 2.5.1 |

The driver (jar) can be downloaded from [mariadb connector download](https://mariadb.com/products/connectors-plugins)
or maven : 
```script
<dependency>
	<groupId>org.mariadb.jdbc</groupId>
	<artifactId>mariadb-java-client</artifactId>
	<version>2.5.1</version>
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
        <version>2.6.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## Documentation

For a Getting started guide, API docs, recipes,  etc. see the 
* [About MariaDB connector/J](/documentation/about-mariadb-connector-j.creole)
* [Use MariaDB connector/j driver](/documentation/use-mariadb-connector-j-driver.creole)
* [Changelog](/documentation/changelog.creole)
* [Failover and high-availability](/documentation/failover-and-high-availability-with-mariadb-connector-j.creole)


## Contributing
To get started with a development installation and learn more about contributing, please follow the instructions at our 
[Developers Guide.](/documentation/developers-guide.creole)
