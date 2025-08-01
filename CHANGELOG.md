# Change Log

## [3.5.5](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.5.4) (Aug 2025)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.5.4...3.5.5)

#### Issues Resolved

* CONJ-1265 - ensure rollback and release savepoint operation to be sent to server, even when there is no transaction in progress
* CONJ-1270 - forceConnectionTimeZoneToSession doesn't always set the timezone to server


## [3.5.4](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.5.4) (Jun 2025)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.5.3...3.5.4)

#### Key Enhancements

* CONJ-1261 - Added caching option for loadCodecs results to improve performance

#### Issues Resolved

* CONJ-1234 - Fixed incorrect type definitions in DatabaseMetaData.getTypeInfo()
* CONJ-1247 - Resolved potential race condition that could cause NullPointerException
* CONJ-1250 - avoids redundant queries for CallableStatement.getParameterMetaData()
* CONJ-1251 - Fixed SSL configuration issue where zero SSL settings only functioned without explicit SSL configuration
* CONJ-1252 - Resolved GSSAPI authentication error when server exchanges begin with 0x01 byte
* CONJ-1254 - Corrected DatabaseMetadata.getTypeInfo() returning incorrect values for AUTO_INCREMENT, FIXED_PREC_SCALE, and CASE_SENSITIVE fields
* CONJ-1255 - Fixed getString method on BIT(1) fields to properly honor transformedBitIsBoolean configuration
* CONJ-1259 - Enhanced metadata compatibility with MariaDB version 12.0
* CONJ-1260 - Improved performance of DatabaseMetaData.getExportedKeys method
* CONJ-1256 - Fixed issue to ensure correct catalog name is returned


## [3.5.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.5.3) (Mar 2025)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.5.2...3.5.3)

#### Bugs Fixed

* CONJ-1226 Fixed issue where dates containing zero day or month resulted in a DateTimeException
* CONJ-1232 Resolved timestamp string representation incompatibility between versions 2.7 and 3.x
	* see new option oldModeNoPrecisionTimestamp
* CONJ-1226 Fixed incorrect values returned by ResultSet.getColumnType() for unsigned values
* CONJ-1241 Corrected regression in 3.x affecting column metadata for unsigned types
* CONJ-1243 Fixed CallableStatement.getParameterMetadata() returning wrong java.sql.Type for boolean values
* CONJ-1236 Prevented NPE (Null Pointer Exception) after reconnection failure in high availability configurations
* CONJ-1237 Fixed issue with incorrect statements.isClosed value after closing connection
* CONJ-1239 Disabled BULK operations when no parameters are present
* CONJ-1240 Fixed connectivity issues with databases that only accept TLSv1.3
* CONJ-1235 Modified redirection option to enable by default only when SSL is enabled


## [3.5.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.5.2) (Feb 2025)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.5.1...3.5.2)

#### Bugs Fixed

* CONJ-1216 Resolved a performance issue that occurred when batch processing on MySQL and older MariaDB (pre-10.2) servers
* CONJ-1218 Incorrect behavior where XA connections are closed when regular connections are terminated - this is against specifications
* CONJ-1217 The trustCertificateKeyStorePassword alias parameter isn’t taken into account
* CONJ-1221	DatabaseMetadata.getTypeInfo() is missing the data types UUID and VECTOR
* CONJ-1225 System throws an exception prematurely without checking all available connections
* CONJ-1228 result-set.getObject() on BLOB type returns Blob in place of byte[]
* CONJ-660  new `disconnectOnExpiredPasswords` connection option that controls client behavior when connecting with an expired password.
	When set to true (default), the client disconnects if it detects an expired password.
	When false, the client maintains the connection and allows setting a new password.
* CONJ-1229 Permit executeQuery commands to not return a result-set


## [3.5.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.5.1) (Nov 2024)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.5.0...3.5.1)

#### Notable changes

* CONJ-1193 Implement parsec authentication
* CONJ-1207 New HaMode: sequential write, loadbalance read
* CONJ-1208 permit bulk for INSERT ON DUPLICATE KEY UPDATE commands for 11.5.1+ servers

#### Bugs Fixed

* CONJ-1053 Mark waffle-jna dependency optional in module descriptor
* CONJ-1196 setObject on java.util.Date was considered was a java.sql.Date and truncate hour/minutes/seconds/ms while it must be considered like a java.sql.Timestamp
* CONJ-1211 jdbc 4.3 enquoteIdentifier missing validation
* CONJ-1213 sql command ending with semicolon and trailing space are not using bulk


## [3.5.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.5.0) (Oct 2024)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.4.1...3.5.0)

#### Notable changes

~~* CONJ-1193 Parsec authentication implementation~~
* CONJ-1183 permit setting specific truststore

#### Bugs Fixed

* CONJ-1202 Session variable setting must be executed last
* CONJ-1201 incorrect default behavior for forceConnectionTimeZoneToSession
* CONJ-1200 Batch import fails with exception "Unknown command"
* CONJ-1199 option `connectionCollation` addition in order to force collation
* CONJ-1187 Use different exception type for connection timeouts

## [3.4.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.4.1) (Jul 2024)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.4.0...3.4.1)

##### Bugs Fixed

* CONJ-1181 Ensure Prepare cache use schema
* CONJ-1178 DatabaseMetaData.getImportedKeys return different PK_NAME value than getExportedKeys.
* CONJ-1180 Correct DatabaseMeta.getExportedKeys() performances
* CONJ-1185 Android app compatibility, regex CANON_EQ flag not supported
* CONJ-1188 database meta getSQLKeywords listing all reserved key word, not restricted keywords only
* CONJ-1189 implementation of pinGlobalTxToPhysicalConnection for XA Connection
* CONJ-1190 Adding MySQL option 'databaseTerm' as alias for useCatalogTerm for compatibility
* CONJ-1191 slow metadata getImportedKeys when not having database set
* CONJ-685 permit setting sslMode per host
* CONJ-686 Allow mixing TCP and socket hosts in failover configuration
* CONJ-1068 ResultSetMetaData.getColumnTypeName() returns VARCHAR instead of TINYTEXT
* CONJ-1182 missing XA_RBTIMEOUT,XA_RBTIMEOUT and XA_RBDEADLOCK error mapping


## [3.4.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.4.0) (Apr 2024)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.3.3...3.4.0)

##### Notable Changes

* CONJ-981 Add support for connection redirection
* CONJ-1087 handle mariadb-11.1+ transaction_isolation
* CONJ-1100 Be able to filter system tables and views
* CONJ-1105 TLS certificate validation without needs to provide certificate client side
* CONJ-1171 timezone support missing feature
* CONJ-1173 Bulk implementation returning individual results for MariaDB 11.5
* CONJ-1154 avoid unnecessary set transaction isolation queries

##### Bugs Fixed

* CONJ-1103 Connector/J Version 3 Does Not Respect "nullCatalogMeansCurrent" Property
* CONJ-1161 Database connection failing on android
* CONJ-1107 MariaDB Connector 3 no longer supports query timeout with MySQL
* CONJ-1125 Inconsistency in Handling PreparedStatement.executeQuery() between MariaDB and MySQL Connectors
* CONJ-1156 getTables should be ordered as expected
* CONJ-1163 jdbcCompliantTruncation Does Not Appear To Be Working
* CONJ-1164 Variable initialization ahead of LOAD DATA INFILE not possible by validateLocalFileName pattern
* CONJ-1168 useBulkStmts compatibility value with pre 3.2 version
* CONJ-1169 improve Client prepared statement setMaxRows implementation
* CONJ-1170 OFFSET missing from getSQLKeywords
* CONJ-1158 DatabaseMetaData#getFunctions's result not property ordered
* CONJ-1159 DatabaseMetaData#getClientInfoProperties not ordered correctly
* CONJ-1166 Implement connection properties fallbackToSystemKeyStore and fallbackToSystemTrustStore
* CONJ-1174 ConnectorJ gives precision of 20 for signed bigint

## [3.3.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.3.3) (Feb 2024)
[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.3.2...3.3.3)

##### Bugs Fixed

* CONJ-1050 regression in 3.x.y: nonparameterized batch "INSERT INTO products( name ) VALUES ( 'aaaa' )" fails
* CONJ-1150 Error using PrepareStatement.setURL with null url
* CONJ-1152 Improve message when reaching socket timeout during connection initial commands


## [3.3.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.3.2) (Dec 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.3.1...3.3.2)

##### Bugs Fixed

* CONJ-1117 new option `returnMultiValuesGeneratedIds` for connector 2.x compatibility, so getGeneratedKeys() return all
	ids of multi-value inserts
* CONJ-1140 regression caussing ClassCastException on DatabaseMetaData when use with option defaultFetchSize set
* CONJ-1129 Metadata.getPrimaryKeys table comparison using like in place of strict equality
* CONJ-1130 ensuring batch parameter are cleared after SQL Failure
* CONJ-1131 NullPointerException when Calling getGeneratedKeys() after an SQL Failure
* CONJ-1132 Ensuring reseting result for getUpdateCount() after an SQL Failure
* CONJ-1135 ensuring BULK command not used when using INSERT ON DUPLICATE KEY UPDATE in order to always have unique
	affected rows by default
* CONJ-1136 wrong decoding for Resultset.getByte() results for binary varchar fields
* CONJ-1137 ensuring never having NPE in OkPacket when setting auto commit
* CONJ-1138 Inconsistency in Behavior of PreparedStatement After closeOnCompletion() Between MariaDB and MySQL
	Connectors
* CONJ-1049 Metadata getTableTypes result was not ordered by TABLE_TYPE

## [3.3.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.3.1) (Nov 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.3.0...3.3.1)

##### Bugs Fixed

* CONJ-1120 java 8 compatibility error in 3.3.0
* CONJ-1123 missing OSGi javax.crypto dependency
* CONJ-1124 ensure not having OOM when setting huge fetch size
* CONJ-1109 Regression in clearBatch() for parameterized statements
* CONJ-1126 setting fetchSize directly on a ResultSet object does not reflect the expected change
* CONJ-1127 Statement.getResultSetType () failed to change the result set type
* CONJ-1128 Setting Negative Fetch Size on ResultSet Without Throwing Error

## [3.3.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.3.0) (Nov 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.2.0...3.3.0)

##### Notable Changes

* CONJ-1115 Make connector become more virtual-thread friendly
* CONJ-1108 Database metadata listing TEMPORARY tables/sequences
* CONJ-1113 update ed25519 to recent version
* CONJ-1116 Avoid unnecessary synchronization on calendar when no calendar parameter

##### Bugs Fixed

* CONJ-1102 BatchUpdateException.getUpdateCounts() returns SUCCESS_NO_INFO but expects EXECUTE_FAILED

## [3.2.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.2.0) (Aug 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.1.4...3.2.0)

##### Notable Changes

* CONJ-920 Java batched statements with optimistic locking failing. the option `useBulkStmts` is now disable by default,
	a new option `useBulkStmtsForInserts` is enabled by default, permitting using bulk for INSERT commands only. This
	permits optimistic behavior working by default.
* CONJ-1084 When using maxscale 23.08.0+, and a maxscale node fails, connector will now priorize reconnection to the
	maxscale node having less connection, to ensure repartition after failover
* CONJ-1088 Implement `databaseTerm` option for mysql compatibility
* CONJ-1096 adding option `useLocalSessionState` to permit avoiding queries when application only use JDBC methods.

##### Bugs Fixed

* CONJ-1075 LOAD DATA INFILE is broken on windows
* CONJ-1079 getGeneratedKeys after batch will not return all generated id's if first batch command return no generated
	id.
* CONJ-1080 mariadb Java connector sslMode=verify-ca complaining unable to find trust certificate.
* CONJ-1082 Multiple session system variables parsing fails
* CONJ-1083 Using /*client prepare*/ prefix to force client side prepared statement
* CONJ-1091 can't make a connection when the Read Replica DB is in a hang state when SocketTimeout=0 set
* CONJ-1092 ensure respecting server collation
* CONJ-1094 Missing mariadb/mysql collation

## [3.0.11](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.11) (Aug 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.10...3.0.11)

* CONJ-1089 correcting 3.0.10 incompatibility with in java 8

## [2.7.10](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.10) (Aug 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.9...2.7.10)

* CONJ-1091 Ensure setting connectTimeout as timeout for socket timeout until connection is done. This permit to set a
	connectTimeout, while socketTimeout can still be set to 0

## [3.1.4](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.1.4) (Apr 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.1.3...3.1.4)

* CONJ-1065 wrong Resultset.wasNull() for zero-date timestamps
* CONJ-1070 getBlob on TEXT columns throw Exception
* CONJ-1071 Error response during Bulk execution might result in connection wrong state
* CONJ-1067 When some numeric data types are set to UNSIGNED, ResultSetMetaData.getColumnTypeName() does not return
	UNSIGNED

## [3.1.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.1.3) (Mar 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.1.2...3.1.3)

* CONJ-1054 Threadsafety issue when using CredentialPlugin in v3.x
* CONJ-1056 JDBC connector reads incorrect data from unix socket when the text is too large
* CONJ-1057 Wrong decoding of binary time with value "00:00:00"
* CONJ-1058 JDBC 4.3 org.mariadb.jdbc.Statement enquote* methods implementation @peterhalicky
* CONJ-1060 BIT default metadata doesn't take care of transformedBitIsBoolean option
* report 2.7.9 bug fixes CONJ-1062 and CONJ-1063

## [2.7.9](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.9) (Mar 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.8...2.7.9)

* CONJ-1062 correcting TlsSocketPlugin to use Driver classloader
* CONJ-1063 DatabaseMetaData.getTypeInfo() returns wrong value for UNSIGNED_ATTRIBUTE

## [3.1.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.1.2) (Jan 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.1.1...3.1.2)

* CONJ-1040 possible ConcurrentModificationException when connecting
* CONJ-1041 possible ArrayIndexOutOfBoundsException

## [2.7.8](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.8) (Jan 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.7...2.7.8)

* CONJ-1039 setQueryTimeout not honored by CallableStatement for procedures depending on security context
* CONJ-1041 possible ArrayIndexOutOfBoundsException
* CONJ-1023 set missing SSL capability in handshake after SSL exchanges

## [3.1.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.1.1) (Jan 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.1.0...3.1.1)

- 3.0.10 bug fix:
		- CONJ-1023 Connector/J doesn't set SSL cap bit in Handshake Response Packet
		- CONJ-1026 timezone=auto option failure on non-fixed-offset zone machine
		- CONJ-1032 Compatibility for deprecated arguments is case sensitive now
- CONJ-1036 org.mariadb.jdbc.client.socket.impl.PacketWriter.writeAscii() broken in 3.1.0

## [3.0.10](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.10) (Jan 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.9...3.0.10)

* CONJ-1023 Connector/J doesn't set SSL cap bit in Handshake Response Packet
* CONJ-1026 timezone=auto option failure on non-fixed-offset zone machine
* CONJ-1032 Compatibility for deprecated arguments is case sensitive now

## [3.1.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.1.0) (Nov 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.9...3.1.0)

##### Notable Changes

* CONJ-899 Support UUID Object
* CONJ-916 when a failover occurs, log replayed transaction
* CONJ-917 deprecated options use must be logged
* CONJ-992 load balance distribution
* CONJ-1008 default value for socket option useReadAheadInput
* CONJ-1009 improve performance reading big result-set
* CONJ-1014 avoid creating array when receiving server packet
* CONJ-1015 pipelining sending multiple packet to socket

##### Bugs Fixed

* CONJ-1020 java 11 option setting ignored

## [3.0.9](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.9) (Nov 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.8...3.0.9)

* 2.7.7 merge
* CONJ-1012 stored procedure register output parameter as null if set before registerOutParameter command
* CONJ-1017 Calendar possible race condition, cause wrong timestamp setting

## [2.7.7](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.7) (Nov 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.6...2.7.7)

* CONJ-1021 GSSAPI authentication might result in connection reset
* CONJ-1019 DatabaseMetaData.getImportedKeys should return real value for PK_NAME column
* CONJ-1016 avoid splitting BULK command into multiple commands in case of prepareStatement.setNull() use
* CONJ-1011 correcting possible NPE when using statement.cancel() that coincide with statement.close() in another thread
* CONJ-1007 Socket file descriptors are leaked after connecting with unix socket if DB is not up running

## [3.0.8](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.8) (Sept 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.7...3.0.8)

##### Notable Changes

* small performance improvement
		* [CONJ-1010] improve client side prepared parameter parameter substitution

##### Bugs Fixed

* [CONJ-997] regression in 3.x when using option galeraAllowedState resulting in an IndexOutOfBoundsException
* [CONJ-1002] 2nd failover reconnection ignores default database/schema setting when not set by connection string
* [CONJ-1003] replication configuration always use 1st replica on 3.0
* [CONJ-996] BatchUpdateException doesn't inherited the SQLState & vendorCode from the cause SQL exception
* [CONJ-1006] disabling cachePrepStmts with useServerPrepStmts might result in Exception
* [CONJ-1007] Socket file descriptors are leaked after connecting with unix socket if DB is not up running
* [CONJ-1010] improve client side prepare statement parameter substitution
* [CONJ-999] setting createDatabaseIfNotExist option use on read-only server will refuse connection on 3.0

## [3.0.7](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.7) (Jul 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.6...3.0.7)

* [CONJ-993] SQLDataException reading DATA_TYPE on DatabaseMetaData.getTypeInfo() after 3.0.4
* [CONJ-986] Permit specific Statement.setLocalInfileInputStream for compatibility
* [CONJ-987] Version 3.0.0 returns String for VARBINARY instead of byte[] as 2.7.6 did
* [CONJ-989] Binary column read as String
* [CONJ-990] Setting timezone=UTC result in SQLSyntaxErrorException
* [CONJ-991] Regression: binary(16) is returned as String by getObject()
* [CONJ-994] Version 3.x rejects previously accepted boolean string parameter for BOOLEAN field

## [3.0.6](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.6) (Jun 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.5...3.0.6)

* [CONJ-953] PreparedStatement.getGeneratedKeys() returns rows when no keys are generated in insert
* [CONJ-975] ArrayIndexOutOfBoundsException when attempt to getTime() from ResultSet
* [CONJ-976] Improve use of pipelining when allowLocalInfile is enabled
* [CONJ-979] ResultSet.getObject() returns Byte instead of Boolean for tinyint(1)
* [CONJ-980] Permit setObject with java.util.Date parameter
* [CONJ-984] Permit executing initial command with new option `initSql`
* [CONJ-985] ResultSet.getObject() returns ByteSet instead of Byte[] for BIT

## [3.0.5](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.5) (may 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.4...3.0.5)

* [CONJ-956] ArrayIndexOutOfBoundsException when alias length > 250
* [CONJ-947] value after milliseconds precision lost when timestamp is encoded
* [CONJ-949] keep clientCertificateKeyStoreUrl and clientCertificateKeyStoreUrl aliases
* [CONJ-950] metadata TEXT/TINYTEXT/MEDIUMTEXT/LONGTEXT wrong column type and length
* [CONJ-954] java.time.OffsetDateTime not supported
* [CONJ-958] compatibility with 2.7: now loop through hosts when multiple host without failover mode
* [CONJ-959] java.time.Instant not supported
* [CONJ-961] LOAD DATA LOCAL INFILE was disable by default
* [CONJ-962] resultset for negative TIME value return erronous LocalDateTime values
* [CONJ-965] better error message when not loading serverSslCert file
* [CONJ-967] clearParameters() breaks validity when using output parameters in stored procedures
* [CONJ-969] org.mariadb.jdbc.ClientPreparedStatement is missing a toString implementation, useful for logging

## [3.0.4](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.4) (Mar 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.3...3.0.4)

* [CONJ-915] javadoc addition
* [CONJ-921] DatabaseMetadata#getTables with null value for tableNamePattern throws Syntax error
* [CONJ-922] DECIMAL overflow for long/int/short not throwing exception
* [CONJ-924] NULL column type might result in java.lang.IllegalArgumentException: Unexpected datatype NULL
* [CONJ-926] Client restrict authentication to 'mysql_native_password,client_ed25519,auth_gssapi_client' if
	restrictedAuth parameter is not set
* [CONJ-924] NULL column test correction
* [CONJ-923] correctly return 64 bits generated id / updated rows
* [CONJ-933] load-balancing failover doesn't timeout
* [CONJ-935] Connection.getMetaData() returns MariaDbClob instead of String
* [CONJ-937] metadata getColumnTypeName wrong return type
* [CONJ-934] MariaDbDataSource is sensitive to the order of setting of username and password
* [CONJ-932] Login packet now use recommended length encoded value for connection attributes
* [CONJ-925] missing OSGI infos
* [CONJ-945] ensure retry is limited by retriesAllDown
* [CONJ-940] Permit updating rows when not having primary info on metadata (Xpand)
* [CONJ-939] add Xpand testing

## [3.0.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.3) (Jan 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.2-rc...3.0.3)

* [CONJ-908] correct Connection.prepareStatement(String sql, int[] columnIndexes/String[] columnNames) to return
	generated keys
* [CONJ-909] adding createDatabaseIfNotExist option for 2.x compatibility
* [CONJ-910] permit jdbc:mysql scheme when connection string contains "permitMysqlScheme" for compatibility
* [CONJ-913] Avoid executing additional command on connection for faster connection creation
* [CONJ-912] remove security manager code (JEP 411)
* [CONJ-911] enable keep-alive by default
* failover improvement. some specific commands not in transaction are considered to be replayed in case of failover,
	like PING, PREPARE, ROLLBACK, ...
* CONJ-705 parameter metadata get parameter count even when query cannot be prepared
* prepareStatement.addBatch must initialize with previous set
* Connection.prepareStatement(String sql, int[] columnIndexes/String[] columnNames) must return generated keys
* setting "transaction read only" only for replica
* keeping option interactiveClient for compatibility
* adding option `transactionReplaySize` to control redo cache size
* only set skip metadata connection flag when using binary protocol
* permit getString on a binary object
* compression correction for multi-packet
* COM_RESET_CONNECTION expect a response (ERR_Packet or OK_Packet)
* [CONJ-901] ArrayIndexOutOfBoundsException on StandardReadableByteBuf.readByte error

## [3.0.2-rc](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.2-rc) (31 Aug 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.1-beta...3.0.2-rc)

* CONJ-879 Java 9 module full support
		* Aws IAM credential now use sdk v2 authentication that support java 9 modularity
* CONJ-896 Ensure pool connections validation when a socket fail
* CONJ-897 Ensure having connection's thread id in Exception / debug logs

minor:

* Ensure travis testing for PR/fork

## [3.0.1-beta](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.1-beta) (29 Jul 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/3.0.0-alpha...3.0.1-beta)

* CONJ-879 Provide JPMS module descriptor
* CONJ-880 metadata query performance correction
* CONJ-884 MariaDbPoolDataSource leaks connections when the mariadb server restarts
* CONJ-885 org.mariadb.jdbc.internal.util.pool.Pool swallows SQLException during addConnection
* CONJ-891 getImportedKeys with null catalog restrict result to current database
* CONJ-894 Adding useMysqlMetadata for 2.7 compatibility

## [2.7.4](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.4) (29 Jul 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.3...2.7.4)

* CONJ-890 getImportedKeys/getTables regression returning an empty resultset for null/empty catalog
* CONJ-863 Ensure socket state when SocketTimeout occurs
* CONJ-873 IndexOutOfBoundsException when executing prepared queries using automatic key generation in parallel
* CONJ-884 MariaDbPoolDataSource leaks connections when the mariadb server restarts
* CONJ-893 DatabaseMetaData.getColumns regression causing TINYINT(x) with x > 1 to return BIT type in place of TINYINT
* CONJ-889 CallableStatement using function throw wrong error on getter

## [3.0.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/3.0.0) (3 May 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.2...3.0.0)

This version is a total rewrite of java driver.

* complete rewrite, code clarification, reduced size (15%), more than 90% coverage tested.
* Encoding/decoding implementation are now registred by Codec, permitting codec registry implementation
		* example support of Geometry Object
* Permit authentication plugin restriction by option `restrictedAuth`
* performance improvement:
		* Prepare and execution are now using pipelining when using option `useServerPrepStmts`
		* performance enhancement with MariaDB 10.6 server when using option `useServerPrepStmts`, skipping metadata (
			see https://jira.mariadb.org/browse/MDEV-19237)

correction:

* CONJ-864 includeThreadDumpInDeadlockExceptions always includes the thread dump, even when it is not a deadlock
	exception
* CONJ-858 Properties parameter that differ from string not taken in account

### Easy logging

If using slf4J, just enabled package "org.mariadb.jdbc" log.

level ERROR will log connection error
level WARNING will log query errors
level DEBUG will log queries
level TRACE will log all exchanges with server.

If not using slf4J, console will be used.
If really wanting to use JDK logger, System property "mariadb.logging.fallback" set to JDK will indicate to use common
logging.

### Failover

Failover implementation now permit redoing transaction :
when creating a transaction, all command will be cached, and can be replayed in case of failover.

This functionality can be enabled using option `transactionReplay`.

This is not enabled by default, because this required that application to avoid using non-idempotent commands.

example:

```sql
START TRANSACTION;
select next_val(hibernate_sequence);
INSERT INTO myCar(id, name) VALUE (?, ?) //
with parameters: 1, 'car1'
INSERT INTO myCarDetail(id, carId, name) VALUE (?, ?, ?) //
with parameters: 2, 1, 'detail1'
INSERT INTO myCarDetail(id, carId, name) VALUE (?, ?, ?) //
with parameters: 3, 2, 'detail2'
		COMMIT;
```

### Allow setup of TCP_KEEPIDLE, TCP_KEEPCOUNT, TCP_KEEPINTERVAL

Equivalent options are `tcpKeepIdle`, `tcpKeepCount`, `tcpKeepInterval`
Since available only with java 11, setting this option with java < 11 will have no effect.

## [2.7.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.3) (12 May 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.2...2.7.3)

* CONJ-619 Multiple batch update fails after LOAD DATA LOCAL INFILE
* CONJ-854 LOAD XML INFILE breaks when using LOCAL
* CONJ-855 throwing more specific exception for updatable result-set that can not be updated by ResultSet
* CONJ-857 Remove use of mysql.proc table, relying on information_schema.parameters
* CONJ-864 includeThreadDumpInDeadlockExceptions always includes the thread dump, even when it is not a deadlock
	exception
* CONJ-866 long binary parsing improvement
* CONJ-871 OSGi: Missing Import-Package in Connector/J bundle (javax.sql.rowset.serial)
* CONJ-878 option serverSslCert file location
* CONJ-880 metadata query performance correction
* CONJ-858 Properties.put with object that differ from String supported even if use is not recommended
* CONJ-861 executeBatch must not clear last parameter value.
* CONJ-883 using unix socket, hostname is not mandatory anymore

## [2.7.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.2) (29 Jan. 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.1...2.7.2)

* CONJ-847 NPE at UpdatableResultSet#close
* CONJ-849 driver now doesn't close connection caused java.io.NotSerializableException as a result of incorrect data
	bind to a prepared statement parameter
* CONJ-850 MariaDbResultSetMetaData#getPrecision(int) now returns correct length for character data
* CONJ-851 metadata getBestRowIdentifier incompatibility with MySQL 8 correction
* CONJ-853 Support Aurora cluster custom endpoints
* CONJ-852 ON DUPLICATE KEY detection failed when using new line

## [2.7.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.1) (23 Nov. 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.7.0...2.7.1)

* CONJ-834 use of BULK batch is conditioned by capability, not checking server version
* CONJ-835 GSS Imports set in OSGI Bundle
* CONJ-839 Wrong exception message when rewriteBatchedStatements is enabled
* CONJ-841 ResultSetMetaData::getColumnTypeName() returns incorrect type name for LONGTEXT
* CONJ-842 Byte array parameters are now send as long data
* CONJ-837 prepared statement cache leak on ResultSet CONCUR_UPDATABLE concurrency
* CONJ-843 ParameterMetaData::getParameterType for CallableStatement parameter return expected "BINARY" value for BINARY
	type

minor:

* CONJ-845 test suite now test SkySQL with replication setting
* CONJ-838 have a 'replica' alias for 'slave' connection option

## [2.7.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.7.0) (24 Sep. 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.6.2...2.7.0)

* CONJ-805 maxFieldSize string truncation occurs on bytes' length, not character length
* CONJ-807 Correcting possible Get Access Denied error if using multiple classloader
* CONJ-810 normalization of resultset getDate/getTime of timestamp field.
* CONJ-812 DatabaseMetadata.getBestRowIdentifier and getMaxProcedureNameLength correction
* CONJ-813 setConfiguration not being called on classes that extend ConfigurableSocketFactory
* CONJ-816 Table with primary key with DEFAULT function can be inserted for 10.5 servers
* CONJ-817 Switched position of REMARKS and PROCEDURE_TYPE in the getProcedures result
* CONJ-820 MySQLPreparedStatement.setObject can now handle java.lang.Character type
* CONJ-828 new option `ensureSocketState` to ensure protocol state
* CONJ-829 Option to cache callablestatement is now disabled by default
* CONJ-830 connector now throw a better error if SSL is mandatory and server doesn't support SSL
* CONJ-814 Small possible improvement of getCrossReference, getExportedKeys and getImportedKey
* CONJ-825 XAResource.isSameRM implementation

## [2.6.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.6.2) (23 Jul. 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.6.1...2.6.2)

* CONJ-804 - Automatic charset correction
* CONJ-809 - SelectResultSet's (ResultSet)MetaData always indicates all columns to be readonly
* CONJ-802 - Version parsing depending on Classloader might result in connection Exception

## [2.6.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.6.1) (23 Jun. 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.6.0...2.6.1)

* CONJ-781 - DatabaseMetaData.supportsMultipleResultSets() now return correctly true.
* CONJ-791 - Using CallableStatement.getTimestamp() can't get data correctly
* CONJ-705 - ParameterMetadata now return parameterCount() even if no information
* CONJ-775 - avoid a NPE for malformed "jdbc:mariadb:///" connection string.
* CONJ-776 - Temporal Data Tables are not listed in metadata
* CONJ-785 - corrected escape sequence for multiple backslash escape
* CONJ-786 - Connection.setReadOnly(true ) with option `assureReadOnly` now force read only connection even for mono
	server*
* CONJ-795 - permit resultset.getRow() for TYPE_FORWARD_ONLY when streaming
* CONJ-797 - Connector set UTF8mb4 equivalent in case of server configured with UTF8mb3 collation
* CONJ-800 - implement Statement setEscapeProcessing to avoid escape
* CONJ-801 - possible race condition using resultset getter using label
* CONJ-778 - Missing import org.osgi.service.jdbc in Import-Package clause of the OSGi manifest
* CONJ-779 - Logic error in stop() method of OSGi bundle activator
* CONJ-780 - Logic error in implementation of OSGi DataSourceFactory (MariaDbDataSourceFactory)
* CONJ-788 - resultset metadata always indicate that column is writable even if not
* CONJ-789 - ensure connection reference removal on (prepared)Statement close
* CONJ-782 - SkySQL testing

## [2.6.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.6.0) (19 Mar. 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.5.4...2.6.0)

* CONJ-768 - Check Galera allowed state when connecting when option `galeraAllowedState` is set, and not only on
	validation
* CONJ-759 - on failover, catalog changed might not be set when automatically recreating a connection.
* CONJ-761 - remove unnecessary dependencies for fedora tar creation
* CONJ-763 - Custom SocketFactory now can change options
* CONJ-764 - DatabaseMetaData.getExportedKeys should return "PRIMARY" for PK_NAME column
* CONJ-765 - Allow MariaDbDatabaseMetaData#getExportedKeys to return the exported keys for all tables
* CONJ-766 - Adding a socket timeout until complete authentication, to avoid hangs is server doesn't support pipelining
* CONJ-767 - permit using Aurora RO endpoint
* CONJ-771 - enablePacketDebug must not reset stack on failover
* CONJ-772 - JDBC Conversion Function support parsing correction

## [2.5.4](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.5.4) (27 Jan. 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.5.3...2.5.4)

* CONJ-756 - Logging correction when using enablePacketDebug option
* CONJ-755 - permits avoiding setting session_track_schema with new option `trackSchema`

## [2.5.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.5.3) (07 Jan. 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.5.2...2.5.3)

* CONJ-752 - Manifest file wrong entry - thanks to Christoph Läubrich
* CONJ-750 - protocol error when not setting database with maxscale
* CONJ-747 - JDBC Conversion Function fast-path skipped, always using longer implementation

## [2.5.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.5.2) (22 Nov. 2019)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.5.1...2.5.2)

* CONJ-745 - use pool reset only for corrected COM_RESET_CONNECTION
* CONJ-743 - byte signed value wrong serialization for text protocol
* CONJ-742 - ensure plugin using Driver classloader

## [2.5.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.5.1) (15 Oct. 2019)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.5.0...2.5.1)

* CONJ-736 - OSGI compliance
* CONJ-737 - Error packet caching_sha2_password not handled when not having a password
* CONJ-738 - PAM authentication multiple exchanges permitting multiple step in connection string
* CONJ-735 - Multi insert regression correction returning multi generated keys

## [2.5.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.5.0) (02 Oct. 2019)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.4.4...2.5.0)

* CONJ-663 - Client authentication plugins are now defined as services. The driver has 2 new
	plugins `caching_sha2_password` and `sha256_password plugin` for MySQL compatibility
* CONJ-733 - Credential service: AWS IAM authentication
* CONJ-727 - Support configuration of custom SSLSocketFactory
* CONJ-561 - JDBC 4.3 partial implementation java.sql.Statement methods isSimpleIdentifier, enquoteIdentifier,
	enquoteLiteral and enquoteNCharLiteral
* CONJ-692 - ConnectionPoolDataSource interface addition to MariaDbPoolDataSource
* CONJ-563 - closing possible option batch thread on driver registration.
* CONJ-732 - Driver getPropertyInfo returns no options' information when url is empty
* CONJ-734 - DatabaseMetaData.getSchemaTerm now return "schema", not empty string

## [2.4.4](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.4.4) (14 Sep. 2019)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.4.3...2.4.4)

* CONJ-724 - Do not ignore the Calendar parameter in ResultSet#getTime(int, Calendar)
* CONJ-725 - Connection Failure when using PAM authenticated user on 10.4 MariaDB server
* CONJ-729 - master-slave regression: commit on read-only server Executed only when there is an active transaction on
	master connection
* CONJ-726 - removing possible NPE after failover on aurora cluster

## [2.4.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.4.3) (02 Jul. 2019)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.4.2...2.4.3)

* CONJ-717 - conversion function support for other data type than default MariaDB conversion type
* CONJ-722 - Permit suppression of result-set metadata getTableName for oracle compatibility
* CONJ-719 - Saving values using Java 8 LocalTime does not store fractional parts of seconds
* CONJ-716 - Correcting possible NPE on non thread safe NumberFormat (logging)

## [2.4.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.4.2) (17 Jun. 2019)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.4.1...2.4.2)

Security

* CONJ-679 - parse Query when receiving LOAD LOCAL INFILE

Bugs

* CONJ-703 - ClassNotFoundException when trying to connect using two-authentication in an OSGI environment.
* CONJ-711 - Xid format id is unsigned integer, currently sending as signed value.
* CONJ-700 - autoReconnect=true on Basic Failover doesn't reconnect
* CONJ-707 - failover might throw an unexpected exception with using "failover"/"sequential" configuration on socket
	error
* CONJ-709 - includeThreadDumpInDeadlockExceptions is thrown only if option includeInnodbStatusInDeadlockExceptions is
	set
* CONJ-710 - Throw complete stackTrace when having an exception on XA Commands
* CONJ-714 - Error on connection on galera server when in detached mode.
* CONJ-701 - typo in error message in SelectResultSet.java

## [2.4.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.4.1) (15 Mar. 2019)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.4.0...2.4.1)

Evolutions

* misc - enabled running of 'SHOW ENGINE INNODB STATUS' for error code 1213 (@mtykhenko)
* misc - reduce mutex using select @@innodb_read_only for aurora (@matsuzayaws)

Bugs

* misc - updating checkstyle version dependency
* misc - permit using SSL on localsocket
* CONJ-687 - addition of option "useMysqlMetadata" to permit MySQL meta compatibility
* misc - java PID using java 9 ProcessHandle if existing, relying on JNA if present
* CONJ-682 - internal pool correction: when receiving an RST during connection validation, the pool will end up throwing
	connection timeout exception in place of reusing another connection.

## [2.4.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.4.0) (28 Jan. 2019)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.3.0...2.4.0)

Evolutions

* CONJ-675 - permit multiple alternative authentication methods for the same user (future MariaDB 10.4 feature)
* CONJ-678 - permit indication of truststore/keystore type (JKS/PKCS12), then not relying on java default type
* CONJ-378 - GSSAPI: client can provide SPN
* CONJ-667 - Support MYSQL_TYPE_JSON datatype
* CONJ-652 - faster results buffering socket available
* CONJ-659 - improve text performance reading date/time/timestamp resultset
* CONJ-670 - ability to Refresh SSL certificate

New options

| Option               | Description                                                                                                                                                         |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| useReadAheadInput    | use a buffered inputSteam that read socket available data. <br /><i>Default: true</i>                                                                               |
| keyStoreType         | indicate key store type (JKS/PKCS12). default is null, then using java default type.                                                                                |
| trustStoreType       | indicate trust store type (JKS/PKCS12). default is null, then using java default type                                                                               |
| servicePrincipalName | when using GSSAPI authentication, SPN (Service Principal Name) use the server SPN information. When set, connector will use this value, ignoring server information |

Bugs

* CONJ-646 - possible NullPointerException when connection lost to database using aurora configuration with one node
* CONJ-672 - batch using multi-send can hang when using query timeout
* CONJ-544 - disable SSL session resumption when using SSL
* CONJ-589 - correcting Clob.length() for utf8mb4
* CONJ-649 - datasource connectTimeout URL parameter is not honoured
* CONJ-650 - Correction on resultset.getObject(columnName, byte[].class) when value is NULL
* CONJ-665 - old MySQL (<5.5.3) doesn't support utf8mb4, using utf8 on 3 bytes as connection charset by default
* CONJ-671 - MariaDb bulk threads occupy full cpu(99%) while db connections broken
* CONJ-673 - abording a connection while fetching a query still does read whole resultset
* CONJ-669 - SQLSyntaxErrorException when querying on empty column name
* CONJ-674 - make dumpQueriesOnException = false by default as per documentation

minor:

* CONJ-644 - small optimization when validating galera connection
* CONJ-625 - add coverage test
* CONJ-654 - DatabaseMetaData.getDriverName() returns connector/J with a lowercase c

## [2.3.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.3.0) (06 Sep. 2018)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.2.6...2.3.0)

#### [CONJ-398] Improve deadlock debugging capabilities

MariaDB has now 2 new options to permit identifying deadlock :
New options

| Option                                  | Description                                                                                                              |
|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| includeInnodbStatusInDeadlockExceptions | add "SHOW ENGINE INNODB STATUS" result to exception trace when having a deadlock exception.<br /><i>//Default: false</i> |
| includeThreadDumpInDeadlockExceptions   | add thread dump to exception trace when having a deadlock exception.<br /><i>Default: false</i>                          |

#### [CONJ-639] the option "enabledSslProtocolSuites" now include TLSv1.2 by default

previous default value was "TLSv1, TLSv1.1", disabling TLSv1.2 by default, due to a corrected issue (MDEV-12190) with
servers using YaSSL - not openSSL. Server error was .
Now, the default value is "TLSv1, TLSv1.1, TLSv1.2". So TLSv1.2 can be use directly.
Connecting MySQL community server use YaSSL without correction, and connection might result in SSLException: "
Unsupported record version Unknown-0.0".

#### [CONJ-642] disable the option "useBulkStmts" by default

Using useBulkStmts permit faster batch, but cause one major issue : Batch return -1 = SUCCESS_NO_INFO

Different option use this information for optimistic update, and cannot confirm if update succeed or not.
This option still makes sense, since for big batch is way faster, but will not be activated by default.

##= Minor change:

* CONJ-628 - optimization to read metadata faster
* CONJ-637 - java.sql.Driver class implement DriverPropertyInfo[] getPropertyInfo, permitting listing options on
	querying tools
* CONJ-639 - enabledSslProtocolSuites does not include TLSv1.2 by default
* CONJ-641 - update maven test dependencies for java 10 compatibility
* CONJ-643 - PreparedStatement::getParameterMetaData always returns VARSTRING as type resulting in downstream libraries
	interpreting values wrongly

##= Bug correction:

* CONJ-616 - correction on possible NPE on getConnection when using failover configuration and master is down, not
	throwing a proper exception
* CONJ-636 - Error in batch might throw a NPE and not the proper Exception

## [2.2.6](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.2.6) (19 Jul. 2018)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.2.5...2.2.6)

minor change:

* CONJ-623 - Increase connection logging when Primary node connection fails
* CONJ-384 - Permit knowing affected rows number, not only real changed rows

New options
|=useAffectedRows|default correspond to the JDBC standard, reporting real affected rows. if
enable, will report "affected" rows. example : if enabled, an update command that doesn't change a row value will still
be "affected", then report.<br /><i>Default: false. Since 2.2.6</i>

Bug correction:

* CONJ-624 - MariaDbPoolDataSource possible NPE on configuration getter
* CONJ-623 - Increase connection logging when Primary node connection fails
* CONJ-622 - The option "connectTimeout" must take in account DriverManager.getLoginTimeout() when set
* CONJ-621 - wrong escaping when having curly bracket in table/field name
* CONJ-618 - Client preparestatement parsing error on escaped ' / " in query

## [2.2.5](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.2.5) (28 May. 2018)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.2.4...2.2.5)

minor change:

* CONJ-602 - Add server hostname to connection packet for proxy
* CONJ-604 - handle support for mysql 8.0 tx_isolation replacement by transaction_isolation

Bug correction:

* CONJ-613 - Connection using "replication" Parameters fail when no slave is available
* CONJ-595 - Create option to configure DONOR/DESYNCED Galera nodes to be unavailable for load-balancing
* CONJ-605 - Newlines where breaking calling stored procedures
* CONJ-609 - Using getDate with function DATE_ADD() with parameter using string format where return wrong result using
	binary protocol
* CONJ-610 - Option "allowMasterDownConnection" improvement on connection validation and Exceptions on master down

## [2.2.4](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.2.4) (04 May. 2018)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.2.3...2.2.4)

Minor changes:

* CONJ-580 - Some options are missing in documentation like default 'autocommit' value
* CONJ-597 - Internal exchanges send utf8mb4 with server even if default server collation is not utf8/utf8mb4
* CONJ-600 - Upgrading non-mandatory Waffle dependency to 1.9.0 (windows GSSAPI authentication)
* CONJ-575 - test addition to ensure YaSSL downgrade TLSv1.2 protocol to TLSv1.1

## [2.2.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.2.3) (08 Mar. 2018)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.2.2...2.2.3)

Bug correction:

* CONJ-583 - possible hang indefinitely using master/slave configuration and failover occur
* CONJ-586 - erroneous transaction state when first command result as error
* CONJ-587 - using allowMasterDownConnection option can lead to NPE when using setReadOnly()
* CONJ-588 - using option 'allowMasterDownConnection' won't permit connecting if master is down
* CONJ-534 - Connection.isValid() must be routed to Master and Slave connections to avoid any server timeout

## [2.2.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.2.2) (20 Feb. 2018)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.2.1...2.2.2)

Bug correction:

* CONJ-564 - Never ever throw an instance of java.lang.Error
* CONJ-579 - Keywords missing from DatabaseMetaData.getSQLKeywords()
* CONJ-567 - UrlParser.initialUrl gets overwritten
* CONJ-571 - Permit java 9 serialization filtering
* CONJ-574 - forcing using toLowerCase/toUpperCase with Locale.ROOT
* CONJ-560 - Automatic module name for java 9
* CONJ-578 - windows testing using all mariadb server
* CONJ-570 - Add tests for 10.3.3 INVISIBLE column

## [2.2.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.2.1) (22 Dec. 2017)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.2.0...2.2.1)

* CONJ-501 - provide support for authentication plugin ed25519

Bug correction:

* CONJ-529 - failover : the driver will pause for 250ms if no servers are available before attempting to reconnect
	another time
* CONJ-548 - don't use COM_STMT_BULK_EXECUTE for INSERT ... SELECT statements
* CONJ-549 - correction on connection reset when using MariaDbPoolDataSource with options useServerPrepStmts and
	useResetConnection enabled
* CONJ-555 - failover caused by client timeout must not reuse connection
* CONJ-558 - removing extra ".0" to resultset.getString() value for FLOAT/DOUBLE fields
* CONJ-550 - fetching state correction when reusing statement without having read all results
* CONJ-553 - RejectedExecutionException was thrown when having large amount of concurrent batches

## [2.2.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.2.0) (08 Nov. 2017)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.1.2...2.2.0)

Evolutions:

#### CONJ-522 - Pool datasource implementation

MariaDB has now 2 different Datasource implementation :

* MariaDbDataSource : Basic implementation. A new connection each time method getConnection() is called.
* MariaDbPoolDataSource : Connection pooling implementation. MariaDB Driver will keep a pool of connection and borrow
	Connections when asked for it.

New options

| Option             | Description                                                                                                                                                                                                                                                                                                                                                             |
|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| pool               | Use pool. This option is useful only if not using a DataSource object, but only connection object. <br /><i>Default: false. since 2.2.0</i>                                                                                                                                                                                                                             |
| poolName           | Pool name that will permit to identify thread.<br />default: auto-generated as MariaDb-pool-<pool-index> <i>since 2.2.0</i>                                                                                                                                                                                                                                             |
| maxPoolSize        | The maximum number of physical connections that the pool should contain. <br /><i>Default: 8. since 2.2.0</i>                                                                                                                                                                                                                                                           |
| minPoolSize        | When connection are removed since not used since more than "maxIdleTime", connections are closed and removed from pool. "minPoolSize" indicate the number of physical connections the pool should keep available at all times. Should be less or equal to maxPoolSize.<br /><i>Default: maxPoolSize value. Since 2.2.0</i>                                              |
| poolValidMinDelay  | When asking a connection to pool, Pool will validate connection state. "poolValidMinDelay" permit to disable this validation if connection has been borrowed recently avoiding useless verification in case of frequent reuse of connection. 0 meaning validation is done each time connection is asked.<br /><i>Default: 1000 (in milliseconds). Since 2.2.0</i>       |
| maxIdleTime        | The maximum amount of time in seconds that a connection can stay in pool when not used. This value must always be below @wait_timeout value - 45s <br /><i>Default: 600 in seconds (=10 minutes), minimum value is 60 seconds. Since 2.2.0</i>                                                                                                                          |
| staticGlobal       | Indicate the following global variable (@@max_allowed_packet,@@wait_timeout,@@autocommit,@@auto_increment_increment,@@time_zone,@@system_time_zone,@@tx_isolation) values won't changed, permitting to pool to create new connection faster.<br /><i>Default: false. Since 2.2.0</i>                                                                                    |
| useResetConnection | When a connection is closed() (give back to pool), pool reset connection state. Setting this option, session variables change will be reset, and user variables will be destroyed when server permit it (MariaDB >= 10.2.4, MySQL >= 5.7.3), permitting to save memory on server if application make extensive use of variables<br /><i>Default: false. Since 2.2.0</i> |

Other evolutions:

* CONJ-530 - Permit Connection.abort() forcing killing the connection, even if connection is stuck in another thread
* CONJ-531 - permit cancelling streaming result-set using Statement.cancel.
* CONJ-495 - Improve reading result-set data
* CONJ-510 - allow execution of read-only statements on slaves when master is down

Bug :

* CONJ-532 - correction Statement.getMoreResults() for multi-queries
* CONJ-533 - PrepareStatement.setTime() may insert incorrect time according to current timezone, time and option "
	useLegacyDatetimeCode"
* CONJ-535 - correction on numerical getter for big BIT data type fields
* CONJ-541 - Fix behavior of ResultSet#relative when crossing result set boundaries

Misc:

* CONJ-469 - Improve Blob/Clob implementation (avoiding array copy from result-set row)
* CONJ-539 - better message when server close connection
* misc - resultset.findColumn method use column name if alias not found
* misc - default option "connectTimeout" value to 30 seconds (was 0 = no timeout)
* misc - ensure that enablePacketDebug option works when timer tick is big

## [2.1.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.1.2) (24 Sep. 2017)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.1.1...2.1.2)

Bug :

* CONJ-525 - Batch result-set return array correction when DELETE statement when bulk option is used
* CONJ-526 - better error message getting metadata information when SQL syntax is wrong
* CONJ-527 - Resultset.last() return wrong value if resultset has only one result
* CONJ-528 - Error executing LOAD DATA LOCAL INFILE when file is larger than max_allowed_packet

## [2.1.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.1.1) (05 Sep. 2017)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.1.0...2.1.1)

Bug :

* CONJ-519 - Updatable result-set possible NPE when same field is repeated.
* CONJ-514 - ResultSet method wasNull() always return true after a call on a "null-date" field binary protocol handling
* CONJ-516 - Permit using updatable result-set when fetching
* CONJ-511 - Add legacy SSL certificate Hostname verification with CN even when SAN are set
* CONJ-515 - Improve MariaDB driver stability in case JNA errors

misc :

* correct typo in error message when setting wrong parameter
* add trace to HostnameVerifier implementation
* handling connection error when no database is provided

## [2.1.0](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.1.0) (29 Jul. 2017)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.0.3...2.1.0)

##= CONJ-422 : verification of SSL Certificate Name Mismatch
When using ssl, driver check hostname against the server's identity as presented in the server's Certificate (checking
alternative names or certificate CN) to prevent man-in-the-middle attack.

A new option "disableSslHostnameVerification" permit to deactivate this validation.
|=disableSslHostnameVerification| When using ssl, driver check hostname against the server's identity as presented in
the server's Certificate (checking alternative names or certificate CN) to prevent man-in-the-middle attack. This option
permit to deactivate this validation.<br />//Default: false. Since 2.1.0//

##= CONJ-400 - Galera validation
When configuration with multi-master, Connection.isValid() will not only validate connection, but primary state.
A connection to a node that is not in primary mode will return false (then for pool, connection will be discarded)

##= CONJ-322 - ResultSet.update* methods implementation
ResultSet.update* methods aren't implemented
statement using ResultSet.CONCUR_UPDATABLE must be able to update record.
exemple:
{{{
Statement stmt = con.createStatement(
ResultSet.TYPE_SCROLL_INSENSITIVE,
ResultSet.CONCUR_UPDATABLE);
ResultSet rs = stmt.executeQuery("SELECT age FROM TABLE2");
// rs will be scrollable, will not show changes made by others,
// and will be updatable
while(rs.next()){
//Retrieve by column name
int newAge = rs.getInt(1) + 5;
rs.updateDouble( 1 , newAge );
rs.updateRow();
}
}}}

##= CONJ-389 - faster batch insert
Use dedicated [COM_STMT_BULK_EXECUTE |https://mariadb.com/kb/en/mariadb/com_stmt_bulk_execute/] protocol for batch
insert when possible.
(batch without Statement.RETURN_GENERATED_KEYS and streams) to have faster batch.
(significant only if server MariaDB &ge; 10.2.7)

A new option "useBulkStmts" permit to deactivate this functionality.
|=useBulkStmts| Use dedicated COM_STMT_BULK_EXECUTE protocol for batch insert when possible. (batch without
Statement.RETURN_GENERATED_KEYS and streams) to have faster batch. (significant only if server MariaDB &ge;
10.2.7)<br />//Default: true. Since 2.1.0//

other evolution

* CONJ-508 - Connection.getCatalog() optimisation for 10.2+ server using new session_track_schema capabilities
* CONJ-492 - Failover handle automatic reconnection on KILL command

Bug

* CONJ-502 - isolation leak when using multiple pools on same VM on failover
* CONJ-503 - regression on aurora Connection.isReadOnly()
* CONJ-505 - correcting issue that ended throwing "Unknown prepared statement handler given to mysqld_stmt_execute"
* CONJ-496 - return rounded numeric when querying on a decimal field in place of throwing an exception for compatibility

## [2.0.3](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.0.3) (27 Jun. 2017)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.0.2...2.0.3)

Bug

* CONJ-473 - when useServerPrepStmts is not set, the PREPARE statement must not be cached.
* CONJ-494 - Handle PrepareStatement.getParameterMetaData() if query cannot be PREPAREd
* CONJ-497 - escape string correction for big query

## [2.0.2](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.0.2) (05 Jun. 2017)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.0.1...2.0.2)

Bug

* CONJ-490 - DataSource connectTimeout is in second, but was set on socket timeout that is in milliseconds
* CONJ-481 - Buffer overrun reading ResultSet when using option "useServerPrepStmts"
* CONJ-470 - Error when executing SQL contains "values" and rewriteBatchedStatements=true
* CONJ-471 - PK_NAME returned by DatabaseMetadata.getPrimaryKeys() should not be null
* CONJ-477 - Aurora not compatible with option usePipelineAuth. Now automatically disabled when aurora is detected
* CONJ-479 - ArrayIndexOutOfBoundsException on connect to MySQL 5.1.73
* CONJ-480 - Access denied error on connect to MySQL 5.1.73
* CONJ-483 - Wrong content of DEFERRABILITY column in MariaDbDatabaseMetaData
* CONJ-487 - No timeout exception on Client PrepareStatement
* CONJ-489 - javax.transaction.xa.XAException message error truncated ( near '0x )

Task

* CONJ-478 - Change CI tests to use maxscale 2.1 version
* CONJ-482 - Connection.setNetworkTimeout don't throw exception if no executor
* CONJ-488 - Use java.net.URL to read keyStore and trustStore again

## [2.0.1](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.0.1) (10 May. 2017)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-j/compare/2.0.0-RC...2.0.1)

* CONJ-467 - changing database metadata to 4.2
* CONJ-460 - Query that contain multiqueries with fetch and EOF deprecation failed
* CONJ-464 - Using of "slowQueryThresholdNanos" option with value > Integer.MAX_VALUE results in class cast exception
* CONJ-452 - correcting inline ssl server certificate parsing
* CONJ-461 - LAST_INSERT_ID() validation check correction for rewrite statement
* CONJ-465 - new option "enablePacketDebug"

New Options :
|=enablePacketDebug|Driver will save the last 16 MySQL packet exchanges (limited to first 1000 bytes).<br />Hexadecimal
value of this packet will be added to stacktrace when an IOException occur.<br />This options has no performance
incidence (< 1 microseconds per query) but driver will then take 16kb more memory.//Default: true. Since 1.6.0//|

* CONJ-468 - autoIncrementIncrement value loaded during connection, avoiding a query for first statement for rewrite

## [2.0.0-RC](https://github.com/mariadb-corporation/mariadb-connector-j/tree/2.0.0-RC) (20 Apr. 2017)

Release candidate version.

Java 8 is now minimum required version.

##= CONJ-318 : Handle CLIENT_DEPRECATE_EOF flag
Implement some protocol changes that permit to save some bytes.(part of https://jira.mariadb.org/browse/MDEV-8931).

##= CONJ-341 : handle SERVER_SESSION_STATE_CHANGE status flag
With server with version MariaDB 10.2, MySQL 5.7, ensure driver state :

- driver does now always get current database, even database is changed by query.
- when using rewriteBatchedStatements does return correct autoincrement ids even when session variable
	@auto_increment_increment has change during session.

##= CONJ-393 : improve setQueryTimeout to use SET STATEMENT max_statement_time

Previous implementation of query timeout handling (using Statement.setQueryTimeout) will create an additional thread
with a scheduler.
When timeout is reached, a temporary connection will be created to permit executing "KILL
QUERY <current connection id>", then closing the temporary connection.
When query ended before timeout, the scheduled task will be canceled.

If server is > 10.1.2, query timeout will be handle server side using "SET MAX_STATEMENT_TIME FOR" command.

##= [CONJ-315]

Closing a Statement that was fetching a result-set (using Statement.setFetchSize) and all rows where not read at the
time of closing, a kill query command
will be executed on close, to avoid having to parse all remaining results.

##= [CONJ-442]
Memory optimization : streaming query.
Very big command now doesn't use any intermediate buffer. Commands are sent directly to socket avoiding using memory,
This permit to send very large object (1G) without using any additional memory.

##= [CONJ-366]
Faster connection : bundle first commands in authentication packet
Driver execute different command on connection. Those queries are now send using pipeline (all queries are sent, then
only all results are reads).

New Options :
|=usePipelineAuth|Fast connection creation.//Default: true. Since 2.0.0//|

##= [CONJ-368]
Parsing row result optimisation to avoid creating byte array to the maximum for faster results and less memory use.

##= Remaining JDBC 4.2 missing implementation :

- CONJ-414 - support for large update count [CONJ-414]
- CONJ-409 - PrepareStatement.setObject(...) support for with java 8 temporal object.
- CONJ-411 - support for Statement maxFieldSize

##= Misc

* CONJ-443 - NullpointerException when making concurrent procedure calls
* CONJ-391 - Improve connection using SELECT in place of SHOW to avoid creating a mutex server side.
* CONJ-402 - tcpKeepAlive option now default to true.
* CONJ-448 - QueryException: Incorrect arguments to mysqld_stmt_execute on inserting an "emptyString"-Lob with JPA
* CONJ-451 - Respect type parameter of ResultSet.getObject with type
* CONJ-455 - MetaData : tinyInt1isBit doesn't work properly in TINYINT(1) column that is marked as UNSIGNED
* CONJ-450 - NPE on setClientInfo if value is an empty string
* CONJ-457 - trustStore : Retain leading slash when trust store beings with 'file:///'
* CONJ-160 - ConnectionPool test using hikariCP
* CONJ-307 - valid connector java 9 early access
* CONJ-402 - make tcpKeepAlive option default to true
* CONJ-411 - Implement Statement maxFieldSize
* CONJ-449 - Permit CallableStatement streaming

## 1.5.9

* CONJ-212 : Implement password encoding charset option
* CONJ-423 : Permit to have MySQL driver and MariaDB driver in same classpath
* CONJ-431 : multi-values queries return only one generated key
* CONJ-434 : 1.5.8 regression : ResultSet returns duplicate entries when using fetchsize
* CONJ-437 : ResultSet.getString on field with ZEROFILL doesn't have the '0' leading chars when using binary protocol
* CONJ-435 : avoid "All pipe instances are busy" exception on multiple connections to the same named pipe
* CONJ-446 : ResultSet first() throw an exception for scroll type if TYPE_FORWARD_ONLY when streaming
* CONJ-440 : handle very big COM_STMT_SEND_LONG_DATA packet (1Gb)
* CONJ-429 : ResultSet.getDouble/getFloat may throw a NumberFormatException
* CONJ-438 : using option rewriteBatchedStatements, permit rewrite when query has column/table that contain 'select'
	keyword.

## 1.5.8

* CONJ-424 : getGeneratedKeys() on table without generated key failed on second execution
* CONJ-412 : Metadata take in account tinyInt1isBit in method columnTypeClause
* CONJ-418 : ResultSet.last() isLast() afterLast() and isAfterLast() correction when streaming
* CONJ-415 : ResultSet.absolute() should not always return true
* CONJ-392 : Aurora cluster endpoint detection fails when time_zone doesn't match system_time_zone
* CONJ-425 : CallableStatement getObject class according to java.sql.Types value
* CONJ-426 : Allow executeBatch to be interrupted
* CONJ-420 : High CPU usage against Aurora after 2 hours inactivity

## 1.5.7

* CONJ-407 : handling failover when packet > max_allowed_packet reset the connection state.
* CONJ-403 : possible NPE on ResultSet.close() correction
* CONJ-405 : Calendar instance not cleared before being used in ResultSet.getTimestamp

## 1.5.6

* CONJ-399 : resultSet getLong() for BIGINT column fails if value is Long.MIN_VALUE in Text protocol
* CONJ-395 : Aurora does not randomise selection of read replicas
* CONJ-392 : Aurora cluster endpoint detection timezone issue
* CONJ-394 : mysql_native_password plugin authentication fail when default-auth set
* CONJ-388 : handle timestamp '0000-00-00 00:00:00' getString()
* CONJ-380 : add maxscale in CI
* CONJ-391 : Use SELECT in place of SHOW command on connection
* CONJ-396 : handling multiple resultSet correctly (was failing if more than 2)

## 1.5.5

* CONJ-386 : Disabling useBatchMultiSend option for Aurora, since can hang connection.
* CONJ-385 : Store procedure with resultSet get wrong getUpdateCount() value (0 in place of -1)
* CONJ-383 : permit OldAuthSwitchRequest protocol (compatibility with 5.5 server using plugin)
* CONJ-382 : Client sockets remain option when server close socket when maximum connections number has been reached
* CONJ-381 : Metadata getProcedureColumns precision's information corrected for date/timestamp/datetime
* CONJ-379 : Metadata TINYTEXT type return Types.LONGVARCHAR instead of Types.VARCHAR
* CONJ-376 : Maxscale compatibility : Permit protocol compression only if server permit it
* CONJ-375 : Load data infile with large files fails with OutOfMemoryError
* CONJ-370 : use KeyStore default property when not using keyStore option
* CONJ-369 : Encoding on clob column when useServerPrepStmts=true
* CONJ-362 : fix a possible race condition MariaDbPooledConnection

## 1.5.4

* CONJ-363 : Connection.getClientInfo implementation correction to follow JDBC rules
* CONJ-361 : PrepareStatement setString() with empty string correction.
* CONJ-360 : replacing ManagementFactory.getRuntimeMXBean() that cause possible slow connection depending on JVM /
	environment
* CONJ-359 : Metadata getColumns(...) resultSet doesnt have "IS_GENERATEDCOLUMN" info

## 1.5.3

* CONJ-358 : Permit using private key with password that differ from keyStore password
* CONJ-356 : secure connection : use KeyStore private key and associate public keys certificates only
* CONJ-342 : Empty clientCertificateKeyStoreUrl option correction
* CONJ-353 : IBM jdk compatibility issue
* CONJ-354 : Streaming issue when using procedures in PrepareStatement/Statement
* CONJ-345 : Regression with using COLLATE keyword in PrepareStatement query
* CONJ-352 : metadata correction on getPrecision() for numeric fields
* CONJ-350 : make prepare fallback to client prepare if query cannot be prepared

## 1.5.2

Release version

* CONJ-331 : clearWarnings() now throw exception on closed connection
* CONJ-299 : PreparedStatement.setObject(Type.BIT, "1") registered as true.
* CONJ-293 : permit named pipe connection without host
* CONJ-333 : ResultSet.getString() of PreparedStatement return NULL When TIME column value=00:00:00

RC corrections

* CONJ-335 : Pool connection may fail to connect with good user
* CONJ-332 : option enabledSslCipherSuites rely on java supportedCipherSuites (replacing enabledCipherSuites)
* UTF-8 conversion correction

## 1.5.1

Release candidate version

### Evolution

#### Aurora host auto-discovery

(CONJ-325)

Aurora now auto discover nodes from cluster endpoint.

##### Aurora endpoints

Every aurora instance has a specific endpoint, i.e. a URL that identify the host. Those endpoints look
like `xxx.yyy.zzz.rds.amazonaws.com`.

There is another endpoint named "cluster endpoint" (format `xxx.cluster-yyy.zzz.rds.amazonaws.com`) which is assigned to
the current master instance and will change when a new master is promoted.

In previous version, cluster endpoint use was discouraged, since when a failover occur, this cluster endpoint can point
for a limited time to a host that isn't the current master anymore. Old recommandation was to list all specific
end-points, like : <br />
{{{
jdbc:mariadb:aurora://a.yyy.zzz.rds.amazonaws.com.com,b.yyy.zzz.rds.amazonaws.com.com/db
}}}
This kind of url string will still work, but now, recommended url string has to use only cluster endpoint :<br/>
{{{
jdbc:mariadb:aurora://xxx.cluster-yyy.zzz.rds.amazonaws.com/db
}}}

Driver will automatically discover master and slaves of this cluster from current cluster end-point during connection
time. This permit to add new replicas to the cluster instance will be discovered without changing driver configuration.

This discovery append at connection time, so if you are using pool framework, check if this framework as a property that
controls the maximum lifetime of a connection in the pool, and set a value to avoid infinite lifetime. When this
lifetime is reached, pool will discard the current connection, and create a new one (if needed). New connections will
use the new replicas.
(If connections are never discarded, new replicas will begin be used only when a failover occur)

### Bugfix

* CONJ-329 and CONJ-330 : rewriteBatchedStatements execute single query exceptions correction.
	<br /><br />

## 1.5.0

Release candidate version

### Use native SSPI windows implementation

CONJ-295.<br />

Java kerberos implementation is not well implemented with windows :

* need a Windows registry entry (
	HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Lsa\Kerberos\Parameters\AllowTGTSessionKey) so windows shared
	current ticket to java.
* java kinit must be executed to create a Ticket.
* restriction when client with local admin rights
* ...

[see openJDK issue](https://bugs.openjdk.java.net/browse/JDK-6722928) for more information

Kerberos GSSAPI implementation on Windows in now based on [Waffle](https://github.com/dblock/waffle) that support
windows SSPI based on [JNA](https://github.com/java-native-access/jna).<br />
if waffle-jna (and dependencies) is on classpath, native implementation will automatically be used.

This removes all those problems

### Support for TLSv1.1 and TLSv1.2

CONJ-249/CONJ-301<br />

Driver before version 1.5 support only TLSv1.<br />
Default supported protocol are now TLSv1 and TLSv1.1, other protocols can be activated by options.

MariaDB and MySQL community server permit TLSv1 and TLSv1.1.<br />
MariaDB server from version 10.0.15 is using the openSSL library permitting TLSv1.2 (>= 5.5.41 for the 5.5 branch).
//YaSSL doesn't support TLSv1.2, so if MariaDB server is build from sources with YaSSL, only TLSv1 and TLSv1.1 will be
available, even for version > 10.0.15//

TLSv1.2 can be enabled by setting option {{{enabledSslProtocolSuites}}} to values {{{"TLSv1, TLSv1.1, TLSv1.2"}}}.

A new option {{{enabledSslCipherSuites}}} permit setting specific cipher.

New Options :
|=enabledSslProtocolSuites|Force TLS/SSL protocol to a specific set of TLS versions (comma separated list). <br />
Example : "TLSv1, TLSv1.1, TLSv1.2"<br />//Default: TLSv1, TLSv1.1. Since 1.5.0//|
|=enabledSslCipherSuites|Force TLS/SSL cipher (comma separated list).<br /> Example : "
TLS_DHE_RSA_WITH_AES_256_GCM_SHA384, TLS_DHE_DSS_WITH_AES_256_GCM_SHA384"<br />//Default: use JRE ciphers. Since
1.5.0//|

### Performance improvement

[CONJ-291]<br />

Different performance improvement have been done :

* Using PreparedStatement on client side use a simple query parser to identify query parameters. This parsing was taking
	up to 7% of query time, reduced to 3%.
* Better UTF-8 decoding avoiding memory consumption and gain 1-2% query time for big String.
* client parsing optimization : rewriteBatchedStatements (insert into ab (i) values (1) and insert into ab (i) values (
		2) rewritten as insert into ab (i) values (1), (2))
			is now 19% faster (Depending on queries 40-50% of CPU time was spend testing that buffer size is big enough to
			contain
			query).
* there was some memory wastage when query return big resultset (> 10kb), slowing query.
* ...

[CONJ-320]
Send X well established MySQL protocol without reading results, and read those X results afterwhile.
Basically that permit to avoid a lot of 'ping-pong' between driver and server.

New Options :
|=useBatchMultiSend|PreparedStatement.executeBatch() will send many QUERY before reading result packets.//Default: true.
Since 1.5.0//|
|=useBatchMultiSendNumber|When using useBatchMultiSend, indicate maximum query that can be sent at a time.<br />
//Default: 100. Since 1.5.0//|

### Prepare + execute in one call

CONJ-296

When using MySQL/MariaDB prepared statement, there will be 3 exchanges with server :

* PREPARE - Prepares statement for execution.
* EXECUTE - Executes a prepared statement preparing by a PREPARE statement.
* DEALLOCATE PREPARE - Releases a prepared statement.

See [Server prepare documentation](https://mariadb.com/kb/en/mariadb/prepare-statement/) for more
information.

PREPARE and DEALLOCATE PREPARE are 2 additional client-server round-trip.
Since MariaDB 10.2, a new functionality named COM-MULTI to permitting to send different task to server in one
round-trip.
Driver is using this functionality to PREPARE and EXECUTE in one client-server round-trip.

### Client logging

Client logging can be enabled, permitting to log query information, execution time and different failover information.
This implementation need the standard SLF4J dependency.

New Options :
|=log|Enable log information. require Slf4j version > 1.4 dependency.<br />//Default: false. Since 1.5.0//|
|=maxQuerySizeToLog|Only the first characters corresponding to this options size will be displayed in logs<br />
//Default: 1024. Since 1.5.0//|
|=slowQueryThresholdNanos|Will log query with execution time superior to this value (if defined )<br />//Default: 1024.
Since 1.5.0//|
|=profileSql|log query execution time.<br />//Default: false. Since 1.5.0//|

### "LOAD DATA INFILE" Interceptors

CONJ-305
LOAD DATA INFILE The fastest way to load many datas is using
query [LOAD DATA INFILE](https://mariadb.com/kb/en/mariadb/load-data-infile/).
<br />Problem is using "LOAD DATA LOCAL INFILE" (ie : loading a file from client), may be a security problem :

* A "man in the middle" proxy server can change the actual file asked from server so client will send a Local file to
	this proxy.
* If someone has can execute query from client, he can have access to any file on client (according to the rights of the
	user running the client process).

See [load-data-infile documentation](./documentation/use-mariadb-connector-j-driver.creole#load-data-infile) for more
information.

Interceptors can now filter LOAD DATA LOCAL INFILE query's file location to validate path / file name.
Those interceptors:

* Must implement interface {{{org.mariadb.jdbc.LocalInfileInterceptor}}}.
* Use [[http://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html|ServiceLoader]] implementation, so
	interceptors classes must be listed in file META-INF/services/org.mariadb.jdbc.LocalInfileInterceptor.

Example:
{{{
package org.project;
public class LocalInfileInterceptorImpl implements LocalInfileInterceptor {
@Override
public boolean validate(String fileName) {
File file = new File(fileName);
String absolutePath = file.getAbsolutePath();
String filePath = absolutePath.substring(0,absolutePath.lastIndexOf(File.separator));
return filePath.equals("/var/tmp/exchanges");
}
}
}}}
file META-INF/services/org.mariadb.jdbc.LocalInfileInterceptor must exist with content
{{{org.project.LocalInfileInterceptorImpl}}}.

You can get rid of defining the META-INF/services file
using [[https://github.com/google/auto/tree/master/service|google auto-service]] framework, permitting to use annotation
{{{@AutoService(LocalInfileInterceptor.class)}}} that will register the implementation as a service automatically.

Using the previous example:
{{{
@AutoService(LocalInfileInterceptor.class)
public class LocalInfileInterceptorImpl implements LocalInfileInterceptor {
@Override
public boolean validate(String fileName) {
File file = new File(fileName);
String absolutePath = file.getAbsolutePath();
String filePath = absolutePath.substring(0,absolutePath.lastIndexOf(File.separator));
return filePath.equals("/var/tmp/exchanges");
}
}
}}}

### Minor evolution

* CONJ-260 : Add jdbc nString, nCharacterStream, nClob implementation

### Bugfix

* CONJ-316 : Wrong Exception thrown for ScrollType TYPE_SCROLL_INSENSITIVE
* CONJ-298 : Error on Callable function exception when no parameter and space before parenthesis
* CONJ-314 : Permit using Call with Statement / Prepare Statement
	<br /><br /><br />

## 1.4.6

* CONJ-293] Permit named pipe connection without host
* CONJ-309] Possible NPE on aurora when failover occur during connection initialisation
* CONJ-312] NPE while loading a null from TIMESTAMP field using binary protocol
* misc] batch with one parameter correction (using rewriteBatchedStatements option)

## 1.4.5

* CONJ-297] Useless memory consumption when using Statement.setQueryTimeout
* CONJ-294] PrepareStatement on master reconnection after a failover
* CONJ-288] using SHOW VARIABLES to replace SELECT on connection to permit connection on a galera non primary node
* CONJ-290] Timestamps format error when using prepareStatement with options useFractionalSeconds and useServerPrepStmts

## 1.4.4

* CONJ-289] PrepareStatement on master reconnection after a failover
* CONJ-288] using SHOW VARIABLES to replace SELECT on connection to permit connection on a galera non primary node

## 1.4.3

* CONJ-284] Cannot read autoincremented IDs bigger than Short.MAX_VALUE
* CONJ-283] Parsing correction on MariaDbClientPreparedStatement - syntax error on insert values
* CONJ-282] Handling YEARs with binary prepareStatement
* CONJ-281] Connector/J is incompatible with Google App Engine correction
* CONJ-278] Improve prepared statement on failover

## 1.4.2

* CONJ-275] Streaming result without result throw "Current position is before the first row"

## 1.4.1

* CONJ-274] correction to permit connection to MySQL 5.1 server
* CONJ-273] correction when using prepareStatement without parameters and option rewriteBatchedStatements to true
* CONJ-270] permit 65535 parameters to server preparedStatement
* CONJ-268] update license header
* misc] when option rewriteBatchedStatements is set to true, correction of packet separation when query size >
	max_allow_packet
* misc] performance improvement for select result.

## 1.4.0

### Complete implementation of fetch size.

CONJ-26
JDBC allows to specify the number of rows fetched for a query, and this number is referred to as the fetch size
Before version 1.4.0, query were loading all results or row by row using Statement.setFetchSize(Integer.MIN_VALUE).
Now it's possible to set fetch size according to your need.
Loading all results for large result sets is using a lot of memory. This functionality permit to save memory without
having performance decrease.

### Memory footprint improvement

CONJ-125
Buffers have been optimized to reduced memory footprint

### CallableStatement  performance improvement.

CONJ-209
Calling function / procedure performance is now optimized according to query. Depending on queries, difference can be up
to 300%.

### Authentication evolution

CONJ-251 Permit now new authentication
possibility : [[https://mariadb.com/kb/en/mariadb/pam-authentication-plugin/|PAM authentication]], and GSSAPI/SSPI
authentication.

GSSAPI/SSPI authentication authentication plugin for MariaDB permit a passwordless login.

On Unix systems, GSSAPI is usually synonymous with Kerberos authentication. Windows has slightly different but very
similar API called SSPI, that along with Kerberos, also supports NTLM authentication.
See more detail
in [[https://github.com/mariadb-corporation/mariadb-connector-j/blob/master/documentation/plugin/GSSAPI|GSSAPI/SSPI configuration]]

### Connection attributes

CONJ-217
Driver information informations are now send
to [[https://mariadb.com/kb/en/mariadb/performance-schema-session_connect_attrs-table/|connection attributes tables]] (
performance_schema must be activated).
A new option "connectionAttributes" permit to add client specifics data.

For example when connecting with the following connection string {{{"jdbc:mariadb://localhost:
3306/testj?user=root&connectionAttributes=myOption:1,mySecondOption:'jj'"}}},
if performance_schema is activated, information about this connection will be available during the time this connection
is active :
{{{
select * from performance_schema.session_connect_attrs where processList_id = 5
+----------------+-----------------+---------------------+------------------+
| PROCESSLIST_ID | ATTR_NAME | ATTR_VALUE | ORDINAL_POSITION |
+----------------+-----------------+---------------------+------------------+
|5 |_client_name |MariaDB connector/J |0 |
|5 |_client_version |1.4.0-SNAPSHOT |1 |
|5 |_os |Windows 8.1 |2 |
|5 |_pid |14124@portable-diego |3 |
|5 |_thread |5 |4 |
|5 |_java_vendor |Oracle Corporation |5 |
|5 |_java_version |1.7.0_79 |6 |
|5 |myOption |1 |7 |
|5 |mySecondOption |'jj'                 |8 |
+----------------+-----------------+---------------------+------------------+
}}}

## Minor evolution

* CONJ-210 : adding a "jdbcCompliantTruncation" option to force truncation warning as SQLException.
* CONJ-211 : when in master/slave configuration, option "assureReadOnly" will ensure that slaves are in read-only mode (
	forcing transaction by a query "SET SESSION TRANSACTION READ ONLY").
* CONJ-213 : new option "continueBatchOnError". Permit to continue batch when an exception occur : When executing a
	batch and an error occur, must the batch stop immediatly (default) or finish remaining batch before throwing
	exception.

## Bugfix

* CONJ-236 : Using a parametrized query with a smallint -1 does return the unsigned value
* CONJ-250 : Tomcat doesn't stop when using Aurora failover configuration
* CONJ-260 : Add jdbc nString, nCharacterStream, nClob implementation
* CONJ-269 : handle server configuration autocommit=0
* CONJ-271 : ResultSet.first() may throw SQLDataException: Current position is before the first row
