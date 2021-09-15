[comment]: <> (<p align="center">)
[comment]: <> (  <a href="http://mariadb.com/">)
[comment]: <> (    <img src="https://mariadb.com/kb/static/images/logo-2018-black.png">)
[comment]: <> (  </a>)
[comment]: <> (</p>)

# SingleStore java connector

## Version: 0.0.1

SingleStore java connector is a JDBC 4.2 compatible driver, used to connect applications developed in Java to SingleStore and MySQL databases. SingleStore Connector/J is LGPL licensed.

## Status
[![Linux Build](https://circleci.com/gh/memsql/S2-JDBC-Connector/tree/master.svg?branch=master)](https://circleci.com/gh/memsql/S2-JDBC-Connector)

[comment]: <> ([![Maven Central]&#40;https://maven-badges.herokuapp.com/maven-central/com.singlestore.jdbc/mariadb-java-client/badge.svg&#41;]&#40;https://maven-badges.herokuapp.com/maven-central/com.singlestore.jdbc/mariadb-java-client&#41;)

[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)

[comment]: <> ([![Codacy Badge]&#40;https://api.codacy.com/project/badge/Grade/be7f4c89d63e496d824e8f365478e8c8&#41;]&#40;https://www.codacy.com/app/diego-dupin/mariadb-connector-j?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=MariaDB/mariadb-connector-j&amp;utm_campaign=Badge_Grade&#41;)
[comment]: <> ([![codecov][codecov-image]][codecov-url])

## Obtaining the driver

For java 8 or more :
(maintenance branch for java 7 is 1.x)

The driver (jar) can be downloaded from [GitHub releases](https://mariadb.com/downloads/#connectors)
or maven : 
```script
<dependency>
	<groupId>com.singlestore</groupId>
	<artifactId>singlestore-jdbc-client</artifactId>
	<version>0.0.1</version>
</dependency>
```

## Documentation

For a Getting started guide, API docs, recipes,  etc. see the 

[comment]: <> (TODO Add documentation links)
[comment]: <> (* [About MariaDB connector/J]&#40;https://mariadb.com/kb/en/about-mariadb-connector-j/&#41;)
[comment]: <> (* [Install driver]&#40;https://mariadb.com/kb/en/installing-mariadb-connectorj/&#41;)
* [Changelog](/CHANGELOG.md)

[comment]: <> (* [Failover and high-availability]&#40;https://mariadb.com/kb/en/failover-and-high-availability-with-mariadb-connector-j/&#41;)


[codecov-image]:https://codecov.io/gh/mariadb-corporation/mariadb-connector-j/branch/master/graph/badge.svg
[codecov-url]:https://codecov.io/gh/mariadb-corporation/mariadb-connector-j
