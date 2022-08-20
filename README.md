# TiDB java connector (Community)

English | [中文](/README-zh.md)

***TiDB java connector (Community)***:

- Is a JDBC 4.2 compatible driver, used to connect applications developed in Java to TiDB databases.
- Is LGPL licensed.
- Based on [MariaDB java connector](https://github.com/mariadb-corporation/mariadb-connector-j) 3.0.7 version.
- Is a ***NON-OFFICIAL*** library.
- Is a more ***AGGRESSIVE*** adaptation of new features of TiDB.
- Unlike [MySQL Connector/J](https://github.com/mysql/mysql-connector-j), this library welcomes pull request from anybody.

## Status
[![Linux Build](https://travis-ci.com/Icemap/tidb-connector-j.svg?branch=master)](https://app.travis-ci.com/github/mariadb-corporation/mariadb-connector-j)
[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)
[![codecov][codecov-image]][codecov-url]

## Obtaining the driver

For java 8 or more :
(maintenance branch for java 7 is 1.x)

The driver (jar) can be downloaded from maven:

**Not prepared now**

```xml
<dependency>
    <groupId>org.tidb.jdbc</groupId>
    <artifactId>mariadb-java-client</artifactId>
    <version>3.0.7</version>
</dependency>
```

## Documentation

For a Getting started guide, API docs, recipes, etc. see here:

- [TiDB Developer Guide](https://docs.pingcap.com/tidb/stable/dev-guide-overview)
- [About MariaDB connector/J](https://mariadb.com/kb/en/about-mariadb-connector-j/)
- [TiDB Docs](https://docs.pingcap.com/tidb/stable)

## Roadmap

- [ ] Remove codes and test cases about `procedures`.
- [ ] Remove codes and test cases about `geometry`.
- [x] Remove codes and test cases about `MariaDB Xpand`.
- [x] Repair `major`, `minor`, `patch` version get logic.
- [ ] Build CI for TiDB.
- [ ] Support close connection for TiDB (Use `KILL TIDB xxx` statement).
- [ ] Support HA for TiDB.
- [ ] Support [Optimistic Transactions and Pessimistic Transactions](https://docs.pingcap.com/tidb/stable/dev-guide-optimistic-and-pessimistic-transaction) for TiDB.
- [ ] Support [Follower Read](https://docs.pingcap.com/tidb/stable/dev-guide-use-follower-read)
- [ ] Support [Stale Read](https://docs.pingcap.com/tidb/stable/dev-guide-use-stale-read)