# TiDB java connector (Community)

[English](/README.md) | 中文

***TiDB Java 连接器 (社区版)***:

- 是一个适配 JDBC 4.2 的驱动, 用来连接 Java 应用和 TiDB 数据库。
- 遵循 LGPL 协议。
- 基于 [MariaDB java connector](https://github.com/mariadb-corporation/mariadb-connector-j) 3.0.7 版本更改。
- 是一个 ***非官方*** 库。
- 是一个更加激进地适配新特性的库，会更积极的适配 TiDB 的新特性。
- 与 [MySQL Connector/J](https://github.com/mysql/mysql-connector-j) 不同，此库欢迎所有人的 PR。

## 状态

[![Linux Build](https://travis-ci.com/Icemap/tidb-connector-j.svg?branch=master)](https://app.travis-ci.com/github/mariadb-corporation/mariadb-connector-j)
[![License (LGPL version 2.1)](https://img.shields.io/badge/license-GNU%20LGPL%20version%202.1-green.svg?style=flat-square)](http://opensource.org/licenses/LGPL-2.1)
[![codecov][codecov-image]][codecov-url]

## 获取途径

Java 8 以上 :

此驱动可在 Maven 中下载：

**还未准备完毕**

```xml
<dependency>
    <groupId>org.tidb.jdbc</groupId>
    <artifactId>mariadb-java-client</artifactId>
    <version>3.0.7</version>
</dependency>
```

## 文档

你可以在这里找到开发者文档，新手文档，API 文档等：

- [TiDB 开发者手册](https://docs.pingcap.com/zh/tidb/stable/dev-guide-overview)
- [关于 MariaDB connector/J](https://mariadb.com/kb/en/about-mariadb-connector-j/)
- [TiDB 文档](https://docs.pingcap.com/zh/tidb/stable)

## 路线图

- [x] 去除关于 `存储过程` 的代码及测试用例。
- [x] 去除关于 `地理信息` 的代码及测试用例。
- [x] 去除关于 `MariaDB Xpand` 的代码及测试用例。
- [x] 去除关于 `READ_UNCOMMITTED` / `SERIALIZABLE` 事务隔离级别的代码及测试用例。
- [x] 去除关于 `XA` 的代码及测试用例。
- [x] 修复 major, minor, patch 版本的获取逻辑。
- [ ] 与 TiDB 集成构建 CI。
- [x] 支持 TiDB 的关闭连接 (使用 `KILL TIDB xxx` 语句)。
- [ ] 支持 TiDB 的高可用。
- [ ] 支持 TiDB 的 [乐观事务和悲观事务](https://docs.pingcap.com/zh/tidb/stable/dev-guide-optimistic-and-pessimistic-transaction)。
- [ ] 支持 TiDB 的 [Follower Read](https://docs.pingcap.com/zh/tidb/stable/dev-guide-use-follower-read)。
- [ ] 支持 TiDB 的 [Stale Read](https://docs.pingcap.com/tidb/stable/dev-guide-use-stale-read)。

## 不支持的 JDBC 配置

- `maxAllowedPacket`: TiDB 的 `max_allowed_packet` 仅为全局，session 级别不可更改。