// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.*;
import org.mariadb.jdbc.export.HaMode;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.integration.Common;
import org.mariadb.jdbc.util.constants.CatalogTerm;

@SuppressWarnings("ConstantConditions")
public class ConfigurationTest {

  @Test
  public void testWrongFormat() {
    Common.assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse("jdbc:mariadb:/localhost/test"),
        "url parsing error : '//' is not present in the url");
  }

  @Test
  public void testParseProps() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/test", null);
    assertEquals(0, conf.socketTimeout());

    Properties props = new Properties();
    props.setProperty("socketTimeout", "50");
    conf = Configuration.parse("jdbc:mariadb://localhost/test", props);
    assertEquals(50, conf.socketTimeout());
  }

  @Test
  public void testCredentialType() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/test?credentialType=");
    assertNull(conf.credentialPlugin());
  }

  @Test
  public void testWrongHostFormat() {
    Common.assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse("jdbc:mariadb://localhost:wrongPort/test"),
        "Incorrect port value : wrongPort");
  }

  @Test
  public void testNoAdditionalPart() throws SQLException {
    assertNull(Configuration.parse("jdbc:mariadb://localhost/").database());
    assertNull(Configuration.parse("jdbc:mariadb://localhost/?socketTimeout=50").user());
    assertNull(Configuration.parse("jdbc:mariadb://localhost").database());
    assertNull(Configuration.parse("jdbc:mariadb://localhost").user());
    assertEquals(
        50,
        Configuration.parse("jdbc:mariadb://localhost?socketTimeout=50&file=/tmp/test")
            .socketTimeout());
    assertNull(Configuration.parse("jdbc:mariadb://localhost?").user());
  }

  @Test
  public void testAliases() throws SQLException {
    assertEquals(
        "someCipher",
        Configuration.parse("jdbc:mariadb://localhost/?enabledSSLCipherSuites=someCipher")
            .enabledSslCipherSuites());
    assertEquals(
        "/tmp/path",
        Configuration.parse("jdbc:mariadb://localhost/?serverRSAPublicKeyFile=/tmp/path")
            .serverRsaPublicKeyFile());
  }

  @Test
  public void testBuild() throws SQLException {
    String base = "jdbc:mariadb://localhost/DB";
    assertEquals(
        Configuration.parse(base).toString(),
        Configuration.parse(base).toBuilder().build().toString());
    String allOptionSet =
        "jdbc:mariadb://host1:3305,address=(host=host2)(port=3307)(type=replica)/db?user=me&password=***&timezone=UTC&autocommit=false&useCatalogTerm=SCHEMA&createDatabaseIfNotExist=true&useLocalSessionState=true&returnMultiValuesGeneratedIds=true&permitRedirect=false&transactionIsolation=REPEATABLE_READ&defaultFetchSize=10&maxQuerySizeToLog=100&maxAllowedPacket=8000&geometryDefaultType=default&restrictedAuth=mysql_native_password,client_ed25519&initSql=SET"
            + " @@a='10'&socketFactory=someSocketFactory&connectTimeout=22&pipe=pipeName&localSocket=localSocket&uuidAsString=true&tcpKeepAlive=false&tcpKeepIdle=10&tcpKeepCount=50&tcpKeepInterval=50&tcpAbortiveClose=true&localSocketAddress=localSocketAddress&socketTimeout=1000&useReadAheadInput=true&tlsSocketType=TLStype&sslMode=TRUST&serverSslCert=mycertPath&keyStore=/tmp&keyStorePassword=MyPWD&keyStoreType=JKS&trustStoreType=JKS&enabledSslCipherSuites=myCipher,cipher2&enabledSslProtocolSuites=TLSv1.2&allowMultiQueries=true&allowLocalInfile=false&useCompression=true&useAffectedRows=true&disablePipeline=true&cachePrepStmts=false&prepStmtCacheSize=2&useServerPrepStmts=true&credentialType=ENV&sessionVariables=blabla&connectionAttributes=bla=bla&servicePrincipalName=SPN&blankTableNameMeta=true&tinyInt1isBit=false&yearIsDateType=false&dumpQueriesOnException=true&includeInnodbStatusInDeadlockExceptions=true&includeThreadDumpInDeadlockExceptions=true&retriesAllDown=10&galeraAllowedState=A,B&transactionReplay=true&pool=true&poolName=myPool&maxPoolSize=16&minPoolSize=12&maxIdleTime=25000&registerJmxPool=false&poolValidMinDelay=260&useResetConnection=true&serverRsaPublicKeyFile=RSAPath&allowPublicKeyRetrieval=true";
    assertEquals(
        Configuration.parse(allOptionSet).toString(),
        Configuration.parse(allOptionSet).toBuilder().build().toString());
  }

  @Test
  public void testDatabaseOnly() throws SQLException {
    assertEquals("DB", Configuration.parse("jdbc:mariadb://localhost/DB").database());
    assertNull(Configuration.parse("jdbc:mariadb://localhost/DB").user());
  }

  @Test
  public void testUrl() throws SQLException {
    Configuration conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306, true)
            .haMode(HaMode.REPLICATION)
            .build();
    assertEquals("jdbc:mariadb:replication://local/DB", conf.initialUrl());
    assertEquals("jdbc:mariadb:replication://local/DB", conf.toString());
    assertEquals(
        Configuration.parse(
            "jdbc:mariadb:replication://address=(host=local)(port=3306)(type=primary)/DB"),
        conf);

    conf =
        new Configuration.Builder()
            .database("DB")
            .addresses(
                HostAddress.from("local", 3306, true), HostAddress.from("host2", 3307, false))
            .haMode(HaMode.REPLICATION)
            .build();

    assertEquals("jdbc:mariadb:replication://local,host2:3307/DB", conf.initialUrl());

    conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306, true)
            .haMode(HaMode.REPLICATION)
            .socketTimeout(50)
            .build();
    assertEquals("jdbc:mariadb:replication://local/DB?socketTimeout=50", conf.initialUrl());

    conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306)
            .addHost("local", 3307)
            .addHost("local", 3308)
            .haMode(HaMode.REPLICATION)
            .socketTimeout(50)
            .build();
    assertEquals(
        "jdbc:mariadb:replication://local,local:3307,local:3308/DB?socketTimeout=50",
        conf.initialUrl());

    conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306)
            .addHost("local", 3307)
            .addHost("local", 3308)
            .haMode(HaMode.LOADBALANCE)
            .socketTimeout(50)
            .build();
    assertEquals(
        "jdbc:mariadb:loadbalance://address=(host=local)(port=3306)(type=primary),address=(host=local)(port=3307)(type=primary),address=(host=local)(port=3308)(type=primary)/DB?socketTimeout=50",
        conf.initialUrl());

    conf =
        new Configuration.Builder()
            .database("DB")
            .addHost("local", 3306, true)
            .haMode(HaMode.REPLICATION)
            .autocommit(false)
            .build();
    assertEquals("jdbc:mariadb:replication://local/DB?autocommit=false", conf.initialUrl());
  }

  @Test
  public void testPipeSocket() throws SQLException {
    String url =
        "jdbc:mariadb:sequential://address=(pipe=Mariadb106),address=(localSocket=/socket),address=(host=local)(port=3306)(type=primary)/DB?socketTimeout=50";
    Configuration conf =
        new Configuration.Builder()
            .database("DB")
            .addPipeHost("Mariadb106")
            .addLocalSocketHost("/socket")
            .addHost("local", 3306)
            .haMode(HaMode.SEQUENTIAL)
            .socketTimeout(50)
            .build();
    assertEquals(url, conf.initialUrl());
    conf = Configuration.parse(url);
    assertEquals(url, conf.initialUrl());
  }

  @Test
  public void testPipeSocketSsl() throws SQLException {
    String url =
        "jdbc:mariadb:sequential://address=(pipe=Mariadb106),address=(localSocket=/socket),address=(host=local)(port=3306)(sslMode=verify-full)(type=primary)/DB?socketTimeout=50";
    Configuration conf =
        new Configuration.Builder()
            .database("DB")
            .addPipeHost("Mariadb106")
            .addLocalSocketHost("/socket")
            .addHost("local", 3306, "verify-full")
            .haMode(HaMode.SEQUENTIAL)
            .socketTimeout(50)
            .build();
    assertEquals(url, conf.initialUrl());
    conf = Configuration.parse(url);
    assertEquals(url, conf.initialUrl());
  }

  @Test
  public void testAcceptsUrl() {
    Driver driver = new Driver();
    assertFalse(driver.acceptsURL(null));
    assertTrue(driver.acceptsURL("jdbc:mariadb://localhost/test"));
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost/test"));
    assertTrue(driver.acceptsURL("jdbc:mysql://localhost/test?permitMysqlScheme"));
  }

  @Test
  public void testConfigurationIsolation() throws Throwable {
    Configuration conf =
        Configuration.parse("jdbc:mariadb://localhost/test?transactionIsolation=REPEATABLE-READ");
    assertSame(TransactionIsolation.REPEATABLE_READ, conf.transactionIsolation());

    conf = Configuration.parse("jdbc:mariadb://localhost/test");
    assertNull(conf.transactionIsolation());

    conf =
        Configuration.parse("jdbc:mariadb://localhost/test?transactionIsolation=repeatable-read");
    assertSame(TransactionIsolation.REPEATABLE_READ, conf.transactionIsolation());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?transactionIsolation=readCommitted");
    assertSame(TransactionIsolation.READ_COMMITTED, conf.transactionIsolation());

    conf =
        Configuration.parse("jdbc:mariadb://localhost/test?transactionIsolation=READ-UNCOMMITTED");
    assertSame(TransactionIsolation.READ_UNCOMMITTED, conf.transactionIsolation());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?transactionIsolation=SERIALIZABLE");
    assertSame(TransactionIsolation.SERIALIZABLE, conf.transactionIsolation());

    try {
      Configuration.parse("jdbc:mariadb://localhost/test?transactionIsolation=wrong_val");
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(
          e.getMessage().contains("Wrong argument value 'wrong_val' for TransactionIsolation"));
    }

    Assertions.assertNull(
        Configuration.parse("jdbc:mysql://localhost/test?transactionIsolation=wrong_val"));

    conf =
        Configuration.parse(
            "jdbc:mysql://localhost/test?transactionIsolation=SERIALIZABLE&permitMysqlScheme");
    assertSame(TransactionIsolation.SERIALIZABLE, conf.transactionIsolation());
  }

  @Test
  public void testSslAlias() throws Throwable {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=verify-full");
    assertSame(SslMode.VERIFY_FULL, conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=verify_full");
    assertSame(SslMode.VERIFY_FULL, conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=trust");
    assertSame(SslMode.TRUST, conf.sslMode());

    try {
      Configuration.parse("jdbc:mariadb://localhost/test?sslMode=wrong_trust");
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Wrong argument value 'wrong_trust' for SslMode"));
    }

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=verify-ca");
    assertSame(SslMode.VERIFY_CA, conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test");
    assertSame(SslMode.DISABLE, conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode");
    assertSame(SslMode.DISABLE, conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=0");
    assertSame(SslMode.DISABLE, conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=1");
    assertSame(SslMode.VERIFY_FULL, conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=true");
    assertSame(SslMode.VERIFY_FULL, conf.sslMode());
  }

  @Test
  public void testSslCompatibility() throws Throwable {
    assertEquals(
        SslMode.VERIFY_FULL, Configuration.parse("jdbc:mariadb://localhost/test?useSsl").sslMode());
    assertEquals(
        SslMode.VERIFY_FULL,
        Configuration.parse("jdbc:mariadb://localhost/test?useSsl=true").sslMode());
    assertEquals(
        SslMode.VERIFY_FULL,
        Configuration.parse("jdbc:mariadb://localhost/test?useSsl=1").sslMode());
    assertEquals(
        SslMode.VERIFY_FULL,
        Configuration.parse("jdbc:mariadb://localhost/test?useSSL=1").sslMode());
    assertEquals(
        SslMode.TRUST,
        Configuration.parse("jdbc:mariadb://localhost/test?useSsl&trustServerCertificate")
            .sslMode());
    assertEquals(
        SslMode.VERIFY_CA,
        Configuration.parse("jdbc:mariadb://localhost/test?useSsl&disableSslHostnameVerification")
            .sslMode());
  }

  @Test
  public void testBooleanDefault() throws Throwable {
    assertFalse(
        Configuration.parse("jdbc:mariadb:///test").includeThreadDumpInDeadlockExceptions());
    assertFalse(
        Configuration.parse("jdbc:mariadb:///test?includeThreadDumpInDeadlockExceptions=false")
            .includeThreadDumpInDeadlockExceptions());
    assertTrue(
        Configuration.parse("jdbc:mariadb:///test?includeThreadDumpInDeadlockExceptions=true")
            .includeThreadDumpInDeadlockExceptions());
    assertTrue(
        Configuration.parse("jdbc:mariadb:///test?includeThreadDumpInDeadlockExceptions")
            .includeThreadDumpInDeadlockExceptions());
  }

  @Test
  public void testOptionTakeDefault() throws Throwable {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/test");
    assertEquals(30_000, conf.connectTimeout());
    assertEquals(250, conf.prepStmtCacheSize());
    assertNull(conf.user());
    assertEquals(0, conf.socketTimeout());
    int initialLoginTimeout = DriverManager.getLoginTimeout();
    DriverManager.setLoginTimeout(60);
    conf = Configuration.parse("jdbc:mariadb://localhost/test");
    assertEquals(60_000, conf.connectTimeout());
    DriverManager.setLoginTimeout(initialLoginTimeout);
  }

  @Test
  public void testOptionParse() throws Throwable {
    Configuration conf =
        Configuration.parse(
            "jdbc:mariadb://localhost/test?user=root&password=toto&createDB=true"
                + "&autoReconnect=true&prepStmtCacheSize=2&connectTimeout=5&socketTimeout=20");
    assertEquals(5, conf.connectTimeout());
    assertEquals(20, conf.socketTimeout());
    assertEquals(2, conf.prepStmtCacheSize());
    assertEquals("true", conf.nonMappedOptions().get("createDB"));
    assertEquals("true", conf.nonMappedOptions().get("autoReconnect"));
    assertEquals("root", conf.user());
    assertEquals("toto", conf.password());
  }

  @Test
  public void nonCaseSensitiveOptions() throws Throwable {
    Configuration conf =
        Configuration.parse(
            "jdbc:mariadb://localhost/test?useR=root&paSsword=toto&createdb=true"
                + "&autoReConnect=true&prepStMtCacheSize=2&ConnectTimeout=5&socketTimeout=20");
    assertEquals(5, conf.connectTimeout());
    assertEquals(20, conf.socketTimeout());
    assertEquals(2, conf.prepStmtCacheSize());
    assertEquals("true", conf.nonMappedOptions().get("createdb"));
    assertEquals("true", conf.nonMappedOptions().get("autoReConnect"));
    assertEquals("root", conf.user());
    assertEquals("toto", conf.password());
  }

  @Test
  public void wrongTypeParsing() {
    Common.assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse("jdbc:mariadb://localhost/test?socketTimeout=20aa"),
        "Optional parameter socketTimeout must be Integer, was '20aa'");
  }

  @Test
  public void testOptionParseSlash() throws Throwable {
    Configuration jdbc =
        Configuration.parse(
            "jdbc:mariadb://127.0.0.1:3306/colleo?user=root&password=toto"
                + "&localSocket=/var/run/mysqld/mysqld.sock");
    assertEquals("/var/run/mysqld/mysqld.sock", jdbc.localSocket());
    assertEquals("root", jdbc.user());
    assertEquals("toto", jdbc.password());
  }

  @Test
  public void testOptionParseIntegerMinimum() throws Throwable {
    Configuration jdbc =
        Configuration.parse(
            "jdbc:mariadb://localhost/test?user=root&autoReconnect=true"
                + "&prepStmtCacheSize=240&connectTimeout=5");
    assertEquals(5, jdbc.connectTimeout());
    assertEquals(240, jdbc.prepStmtCacheSize());
  }

  @Test
  public void testWithoutDb() throws Throwable {
    Configuration jdbc =
        Configuration.parse("jdbc:mariadb://localhost/?user=root&tcpKeepAlive=true");
    assertTrue(jdbc.tcpKeepAlive());
    assertNull(jdbc.database());

    Configuration jdbc2 =
        Configuration.parse("jdbc:mariadb://localhost?user=root&tcpKeepAlive=true");
    assertTrue(jdbc2.tcpKeepAlive());
    assertNull(jdbc2.database());
  }

  @Test
  public void testOptionParseIntegerNotPossible() {
    assertThrows(
        SQLException.class,
        () ->
            Configuration.parse(
                "jdbc:mariadb://localhost/test?user=root&autoReconnect=true&prepStmtCacheSize=-2"
                    + "&connectTimeout=5"));
  }

  @Test()
  public void testJdbcParserSimpleIpv4basic() throws SQLException {
    String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/database";
    Configuration conf = Configuration.parse(url);
    assertEquals("jdbc:mariadb://master,slave1:3307,slave2:3308/database", conf.initialUrl());
    url =
        "jdbc:mariadb://address=(host=master)(port=3306)(type=primary),address=(host=slave1)(port=3307)(type=replica),address=(host=slave2)(port=3308)(type=replica)/database";
    conf = Configuration.parse(url);
    assertEquals(
        "jdbc:mariadb://master,address=(host=slave1)(port=3307)(type=replica),address=(host=slave2)(port=3308)(type=replica)/database",
        conf.initialUrl());
    url =
        "jdbc:mariadb://address=(host=master)(port=3306)(type=master),address=(host=slave1)(port=3307)(type=replica),address=(host=slave2)(port=3308)(type=replica)/database";
    conf = Configuration.parse(url);
    assertEquals(
        "jdbc:mariadb://master,address=(host=slave1)(port=3307)(type=replica),address=(host=slave2)(port=3308)(type=replica)/database",
        conf.initialUrl());
    url = "jdbc:mariadb:replication://master:3306,slave1:3307,slave2:3308/database";
    conf = Configuration.parse(url);
    assertEquals(
        "jdbc:mariadb:replication://master,slave1:3307,slave2:3308/database", conf.initialUrl());
  }

  @Test
  public void testJdbcParserSimpleIpv4basicError() throws SQLException {
    Configuration Configuration = org.mariadb.jdbc.Configuration.parse(null);
    assertNull(Configuration);
  }

  @Test
  public void testJdbcParserSimpleIpv4basicwithoutDatabase() throws SQLException {
    String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertNull(conf.database());
    assertNull(conf.user());
    assertNull(conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("slave1", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave2", 3308, true), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserWithoutDatabaseWithProperties() throws SQLException {
    String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308?autoReconnect=true";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertNull(conf.database());
    assertNull(conf.user());
    assertNull(conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("slave1", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave2", 3308, true), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserWithoutDatabase2WithProperties() throws SQLException {
    String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/?autoReconnect=true";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertNull(conf.database());
    assertNull(conf.user());
    assertNull(conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("slave1", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave2", 3308, true), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserSimpleIpv4Properties() throws SQLException {
    String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/database?autoReconnect=true";

    Properties prop = new Properties();
    prop.setProperty("user", "greg");
    prop.setProperty("password", "pass");
    prop.setProperty("allowMultiQueries", "true");

    Configuration conf = org.mariadb.jdbc.Configuration.parse(url, prop);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertTrue(conf.allowMultiQueries());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("slave1", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave2", 3308, true), conf.addresses().get(2));

    prop = new Properties();
    prop.put("user", "greg");
    prop.put("password", "pass");
    prop.put("allowMultiQueries", true);

    conf = org.mariadb.jdbc.Configuration.parse(url, prop);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertTrue(conf.allowMultiQueries());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("slave1", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave2", 3308, true), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserBooleanOption() {
    String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308?autoReconnect=truee";
    Properties prop = new Properties();
    prop.setProperty("user", "greg");
    prop.setProperty("password", "pass");
    try {
      Configuration.parse(url, prop);
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage()
              .contains(
                  "Optional parameter autoReconnect must be boolean (true/false or 0/1) was"
                      + " \"truee\""));
    }
  }

  @Test
  public void testJdbcParserSimpleIpv4() throws SQLException {
    String url =
        "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/database?user=greg&password=pass";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("slave1", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave2", 3308, true), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserSimpleIpv6() throws SQLException {
    String url =
        "jdbc:mariadb://[2001:0660:7401:0200:0000:0000:0edf:bdd7],[2001:660:7401:200::edf:bdd7]:3307,[2001:660:7401:200::edf:bdd7]-test"
            + "/database?user=greg&password=pass";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(
        HostAddress.from("2001:0660:7401:0200:0000:0000:0edf:bdd7", 3306, true),
        conf.addresses().get(0));
    assertEquals(
        HostAddress.from("2001:660:7401:200::edf:bdd7", 3307, true), conf.addresses().get(1));
    assertEquals(
        HostAddress.from("2001:660:7401:200::edf:bdd7", 3306, true), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserParameter() throws SQLException {
    String url =
        "jdbc:mariadb://address=(type=primary)(port=3306)(host=master1),address=(port=3307)(type=primary)"
            + "(host=master2)(type=replica),address=(type=slave)(host=slave1)(port=3308)/database?user=greg&password=pass";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("master2", 3307, false), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave1", 3308, false), conf.addresses().get(2));

    url =
        "jdbc:mariadb://address=(port=3306)(host=master1),address=(port=3307)"
            + "(host=master2),address=(host=master3)(port=3308)/database?user=greg&password=pass";
    conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("master2", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("master3", 3308, true), conf.addresses().get(2));

    url =
        "jdbc:mariadb:replication://address=(port=3306)(host=master1),address=(port=3307)(host=slave1)"
            + " ,address=(host=slave2)(port=3308)(other=5/database?user=greg&password=pass";
    conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("slave1", 3307, false), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave2", 3308, false), conf.addresses().get(2));
  }

  @Test
  public void address() {
    assertEquals("address=(host=test)(port=3306)", HostAddress.from("test", 3306).toString());
    assertEquals(
        "address=(host=test)(port=3306)(type=replica)",
        HostAddress.from("test", 3306, false).toString());
    assertEquals(
        "address=(host=test)(port=3306)(type=primary)",
        HostAddress.from("test", 3306, true).toString());
  }

  @Test
  public void hostAddressEqual() {
    HostAddress host = HostAddress.from("test", 3306);
    assertEquals(host, host);
    assertNotEquals(null, host);
    assertEquals(HostAddress.from("test", 3306), host);
    assertNotEquals("", host);
    assertNotEquals(HostAddress.from("test2", 3306, true), host);
    assertNotEquals(HostAddress.from("test", 3306, true), host);
    assertNotEquals(HostAddress.from("test", 3306, false), host);
  }

  @Test
  public void testJdbcParserParameterErrorEqual() {
    String wrongIntVal = "jdbc:mariadb://localhost?socketTimeout=blabla";
    Common.assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse(wrongIntVal),
        "Optional parameter socketTimeout must be Integer, was 'blabla'");
    String wrongBoolVal = "jdbc:mariadb://localhost?autocommit=blabla";
    Common.assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse(wrongBoolVal),
        "Optional parameter autocommit must be boolean (true/false or 0/1)");
    String url =
        "jdbc:mariadb://address=(type=)(port=3306)(host=master1),address=(port=3307)(type=primary)"
            + "(host=master2),address=(type=replica)(host=slave1)(port=3308)/database?user=greg&password=pass";
    Common.assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse(url),
        "Invalid connection URL, expected key=value pairs, found (type=)");
    String url2 =
        "jdbc:mariadb://address=(type=wrong)(port=3306)(host=master1),address=(port=3307)(type=primary)"
            + "(host=master2),address=(type=replica)(host=slave1)(port=3308)/database?user=greg&password=pass";
    Common.assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse(url2),
        "Wrong type value (type=wrong) (possible value primary/replica)");
  }

  @Test
  public void testJdbcParserHaModeNone() throws SQLException {
    String url = "jdbc:mariadb://localhost/database";
    Configuration jdbc = Configuration.parse(url);
    assertEquals(jdbc.haMode(), HaMode.NONE);
  }

  @Test
  public void testJdbcParserHaModeLoadReplication() throws SQLException {
    String url = "jdbc:mariadb:replication://localhost/database";
    Configuration jdbc = Configuration.parse(url);
    assertEquals(jdbc.haMode(), HaMode.REPLICATION);
  }

  @Test
  public void testJdbcParserReplicationParameter() throws SQLException {
    String url =
        "jdbc:mariadb:replication://address=(type=primary)(port=3306)(host=master1),address=(port=3307)"
            + "(type=primary)(host=master2),address=(type=replica)(host=slave1)(port=3308)/database"
            + "?user=greg&password=pass&pinGlobalTxToPhysicalConnection&servicePrincipalName=BLA"
            + "&allowPublicKeyRetrieval&serverRSAPublicKeyFile=/tmp/path";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals("BLA", conf.servicePrincipalName());
    assertTrue(conf.allowPublicKeyRetrieval());
    assertEquals("/tmp/path", conf.serverRsaPublicKeyFile());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("master2", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave1", 3308, false), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserReplicationParameterWithoutType() throws SQLException {
    String url = "jdbc:mariadb:replication://master1,slave1,slave2/database";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("slave1", 3306, false), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave2", 3306, false), conf.addresses().get(2));
  }

  /**
   * Conj-167 : Driver is throwing IllegalArgumentException instead of returning null.
   *
   * @throws SQLException if any exception occur
   */
  @Test
  public void checkOtherDriverCompatibility() throws SQLException {
    Configuration jdbc = Configuration.parse("jdbc:h2:mem:RZM;DB_CLOSE_DELAY=-1");
    assertNull(jdbc);
  }

  @Test
  public void checkDisable() throws SQLException {
    Configuration jdbc = Configuration.parse("jdbc:mysql://localhost/test");
    assertNull(jdbc);
  }

  @Test
  public void loginTimeout() throws SQLException {
    Configuration jdbc = Configuration.parse("jdbc:mariadb://localhost/test");
    assertEquals(30000, jdbc.connectTimeout());

    DriverManager.setLoginTimeout(10);
    jdbc = Configuration.parse("jdbc:mariadb://localhost/test");
    assertEquals(10000, jdbc.connectTimeout());

    jdbc = Configuration.parse("jdbc:mariadb://localhost/test?connectTimeout=5000");
    assertEquals(5000, jdbc.connectTimeout());
    DriverManager.setLoginTimeout(0);

    jdbc = Configuration.parse("jdbc:mariadb://localhost/test?connectTimeout=5000");
    assertEquals(5000, jdbc.connectTimeout());
  }

  @Test
  public void checkHaMode() throws SQLException {
    checkHaMode("jdbc:mariadb://localhost/test", HaMode.NONE);
    checkHaMode("jdbc:mariadb:replication://localhost/test", HaMode.REPLICATION);
    checkHaMode("jdbc:mariadb:replication//localhost/test", HaMode.REPLICATION);
    checkHaMode("jdbc:mariadb:failover://localhost:3306/test", HaMode.LOADBALANCE);
    checkHaMode("jdbc:mariadb:loadbalance://localhost:3306/test", HaMode.LOADBALANCE);

    try {
      checkHaMode("jdbc:mariadb:replicati//localhost/test", HaMode.REPLICATION);
      fail();
    } catch (SQLException sqle) {
      assertTrue(
          sqle.getMessage().contains("wrong failover parameter format in connection String"));
    }
  }

  private void checkHaMode(String url, HaMode expectedHaMode) throws SQLException {
    Configuration jdbc = Configuration.parse(url);
    assertEquals(expectedHaMode, jdbc.haMode());
  }

  /**
   * CONJ-452 : correcting line break in connection url.
   *
   * @throws SQLException if any exception occur
   */
  @Test
  public void checkInfileCertificate() throws SQLException {
    String url =
        "jdbc:mariadb://1.2.3.4/testj?user=diego"
            + "&autocommit=true&serverSslCert="
            + "-----BEGIN CERTIFICATE-----\n"
            + "MIIDITCCAgmgAwIBAgIBADANBgkqhkiG9w0BAQUFADBIMSMwIQYDVQQDExpHb29n\n"
            + "bGUgQ2xvdWQgU1FMIFNlcnZlciBDQTEUMBIGA1UEChMLR29vZ2xlLCBJbmMxCzAJ\n"
            + "BgNVBAYTAlVTMB4XDTE3MDQyNzEyMjcyNFoXDTE5MDQyNzEyMjgyNFowSDEjMCEG\n"
            + "A1UEAxMaR29vZ2xlIENsb3VkIFNRTCBTZXJ2ZXIgQ0ExFDASBgNVBAoTC0dvb2ds\n"
            + "ZSwgSW5jMQswCQYDVQQGEwJVUzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
            + "ggEBANA6vS37//gvYOsEXKA9Cnlt/C1Ef/a5zJNahpVxx8HbJn6DF+pQbhHm3o4P\n"
            + "TeZp1HoRg5TRiXOEkNTBmgSQbR2+otM2q2gmkn4XAh0+yXkNW3hr2IydJyg9C26v\n"
            + "/OzFvuLcw9iDBvrn433pDa6vjYDU+wiQaVtr1ItzsoE/kgW2IkgFVQB+CrkpAmwm\n"
            + "omwEze3QFUUznP0PHy3P7g7UVD9u5x3APY6kVt2dq8mnOiLZkyfHHR2j6+j0E73I\n"
            + "k3HQv7D0yRIv3kuNpFgJbITVgDIq9ukWU2XinDHUjguCDH+yQAoQH7hOQlWUHIz8\n"
            + "/TtfZjrlUQf2uLzOWCn5KxfEqTkCAwEAAaMWMBQwEgYDVR0TAQH/BAgwBgEB/wIB\n"
            + "ADANBgkqhkiG9w0BAQUFAAOCAQEArYkBkUvMvteT4fN6lxUkzmp8R7clLPkA2HnJ\n"
            + "7IUK3c5GJ0/blffxj/9Oe2g+etga15SIO73GNAnTxhxIJllKRmmh6SR+dwNMkAvE\n"
            + "xp87/Y6cSeJp5d4HhZUvxpFjaUDsWIC8tpbriUJoqGIirprLVcsPgDjKyuvVOlbK\n"
            + "aQf3fOoBPLspGWHgic8Iw1O4kRcStUGCSCwOtYcgMJEhVqTgX0sTX5BgatZhr8FY\n"
            + "Mnoceo2vzzxgHJU9qZuPkpYDs+ipQjzhoIJaY4HU2Uz4jMptqxSdzsPpC6PAKwuN\n"
            + "+LBCR0B194YbRn6726vWwUUE05yskVN6gllGSCgZ/G8y98DhjQ==\n"
            + "-----END CERTIFICATE-----&sslMode&password=testj&password=pwd2";
    Configuration jdbc = Configuration.parse(url);
    assertEquals("diego", jdbc.user());
    assertEquals(true, jdbc.autocommit());
    assertEquals(
        "-----BEGIN CERTIFICATE-----\n"
            + "MIIDITCCAgmgAwIBAgIBADANBgkqhkiG9w0BAQUFADBIMSMwIQYDVQQDExpHb29n\n"
            + "bGUgQ2xvdWQgU1FMIFNlcnZlciBDQTEUMBIGA1UEChMLR29vZ2xlLCBJbmMxCzAJ\n"
            + "BgNVBAYTAlVTMB4XDTE3MDQyNzEyMjcyNFoXDTE5MDQyNzEyMjgyNFowSDEjMCEG\n"
            + "A1UEAxMaR29vZ2xlIENsb3VkIFNRTCBTZXJ2ZXIgQ0ExFDASBgNVBAoTC0dvb2ds\n"
            + "ZSwgSW5jMQswCQYDVQQGEwJVUzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
            + "ggEBANA6vS37//gvYOsEXKA9Cnlt/C1Ef/a5zJNahpVxx8HbJn6DF+pQbhHm3o4P\n"
            + "TeZp1HoRg5TRiXOEkNTBmgSQbR2+otM2q2gmkn4XAh0+yXkNW3hr2IydJyg9C26v\n"
            + "/OzFvuLcw9iDBvrn433pDa6vjYDU+wiQaVtr1ItzsoE/kgW2IkgFVQB+CrkpAmwm\n"
            + "omwEze3QFUUznP0PHy3P7g7UVD9u5x3APY6kVt2dq8mnOiLZkyfHHR2j6+j0E73I\n"
            + "k3HQv7D0yRIv3kuNpFgJbITVgDIq9ukWU2XinDHUjguCDH+yQAoQH7hOQlWUHIz8\n"
            + "/TtfZjrlUQf2uLzOWCn5KxfEqTkCAwEAAaMWMBQwEgYDVR0TAQH/BAgwBgEB/wIB\n"
            + "ADANBgkqhkiG9w0BAQUFAAOCAQEArYkBkUvMvteT4fN6lxUkzmp8R7clLPkA2HnJ\n"
            + "7IUK3c5GJ0/blffxj/9Oe2g+etga15SIO73GNAnTxhxIJllKRmmh6SR+dwNMkAvE\n"
            + "xp87/Y6cSeJp5d4HhZUvxpFjaUDsWIC8tpbriUJoqGIirprLVcsPgDjKyuvVOlbK\n"
            + "aQf3fOoBPLspGWHgic8Iw1O4kRcStUGCSCwOtYcgMJEhVqTgX0sTX5BgatZhr8FY\n"
            + "Mnoceo2vzzxgHJU9qZuPkpYDs+ipQjzhoIJaY4HU2Uz4jMptqxSdzsPpC6PAKwuN\n"
            + "+LBCR0B194YbRn6726vWwUUE05yskVN6gllGSCgZ/G8y98DhjQ==\n"
            + "-----END CERTIFICATE-----",
        jdbc.serverSslCert());
    assertEquals(SslMode.DISABLE, jdbc.sslMode());
    assertEquals("pwd2", jdbc.password());
  }

  @Test
  public void builder() throws SQLException {
    Configuration conf =
        new Configuration.Builder()
            .addresses(
                HostAddress.from("host1", 3305, true), HostAddress.from("host2", 3307, false))
            .user("me")
            .password("pwd")
            .database("db")
            .socketFactory("someSocketFactory")
            .connectTimeout(22)
            .restrictedAuth("mysql_native_password,client_ed25519")
            .tcpKeepAlive(false)
            .uuidAsString(true)
            .tcpAbortiveClose(true)
            .localSocketAddress("localSocketAddress")
            .socketTimeout(1000)
            .allowMultiQueries(true)
            .allowLocalInfile(false)
            .useCompression(true)
            .blankTableNameMeta(true)
            .credentialType("ENV")
            .sslMode("REQUIRED")
            .enabledSslCipherSuites("myCipher,cipher2")
            .sessionVariables("blabla")
            .tinyInt1isBit(false)
            .yearIsDateType(false)
            .timezone("UTC")
            .dumpQueriesOnException(true)
            .prepStmtCacheSize(2)
            .useAffectedRows(true)
            .useServerPrepStmts(true)
            .connectionAttributes("bla=bla")
            .useBulkStmts(true)
            .autocommit(false)
            .includeInnodbStatusInDeadlockExceptions(true)
            .includeThreadDumpInDeadlockExceptions(true)
            .servicePrincipalName("SPN")
            .defaultFetchSize(10)
            .tlsSocketType("TLStype")
            .maxQuerySizeToLog(100)
            .retriesAllDown(10)
            .galeraAllowedState("A,B")
            .enabledSslProtocolSuites("TLSv1.2")
            .transactionReplay(true)
            .pool(true)
            .poolName("myPool")
            .maxPoolSize(16)
            .minPoolSize(12)
            .maxIdleTime(25000)
            .transactionIsolation("REPEATABLE-READ")
            .keyStore("/tmp")
            .keyStorePassword("MyPWD")
            .keyStoreType("JKS")
            .trustStoreType("JKS")
            .geometryDefaultType("default")
            .registerJmxPool(false)
            .tcpKeepCount(50)
            .tcpKeepIdle(10)
            .tcpKeepInterval(50)
            .poolValidMinDelay(260)
            .useResetConnection(true)
            .useReadAheadInput(true)
            .cachePrepStmts(false)
            .serverSslCert("mycertPath")
            .permitRedirect(false)
            .useLocalSessionState(true)
            .returnMultiValuesGeneratedIds(true)
            .serverRsaPublicKeyFile("RSAPath")
            .allowPublicKeyRetrieval(true)
            .createDatabaseIfNotExist(true)
            .disablePipeline(true)
            .maxAllowedPacket(8000)
            .nullDatabaseMeansCurrent(true)
            .fallbackToSystemKeyStore(false)
            .fallbackToSystemTrustStore(false)
            .initSql("SET @@a='10'")
            .useCatalogTerm("schema")
            .preserveInstants(true)
            .connectionTimeZone("SERVER")
            .forceConnectionTimeZoneToSession(false)
            .connectionCollation("utf8mb4_vietnamese_ci")
            .trustStore("/tmp/file")
            .trustStorePassword("PWD")
            .trustStoreType("jks")
            .build();
    String expected =
        "jdbc:mariadb://host1:3305,address=(host=host2)(port=3307)(type=replica)/db?user=me&password=***&timezone=UTC&connectionCollation=utf8mb4_vietnamese_ci&connectionTimeZone=SERVER&forceConnectionTimeZoneToSession=false&preserveInstants=true&autocommit=false&nullDatabaseMeansCurrent=true&useCatalogTerm=SCHEMA&createDatabaseIfNotExist=true&useLocalSessionState=true&returnMultiValuesGeneratedIds=true&permitRedirect=false&transactionIsolation=REPEATABLE_READ&defaultFetchSize=10&maxQuerySizeToLog=100&maxAllowedPacket=8000&geometryDefaultType=default&restrictedAuth=mysql_native_password,client_ed25519&initSql=SET"
            + " @@a='10'&socketFactory=someSocketFactory&connectTimeout=22&uuidAsString=true&tcpKeepAlive=false&tcpKeepIdle=10&tcpKeepCount=50&tcpKeepInterval=50&tcpAbortiveClose=true&localSocketAddress=localSocketAddress&socketTimeout=1000&useReadAheadInput=true&tlsSocketType=TLStype&sslMode=TRUST&serverSslCert=mycertPath&keyStore=/tmp&trustStore=/tmp/file&keyStorePassword=***&trustStorePassword=***&keyStoreType=JKS&trustStoreType=jks&enabledSslCipherSuites=myCipher,cipher2&enabledSslProtocolSuites=TLSv1.2&fallbackToSystemKeyStore=false&fallbackToSystemTrustStore=false&allowMultiQueries=true&allowLocalInfile=false&useCompression=true&useAffectedRows=true&useBulkStmts=true&disablePipeline=true&cachePrepStmts=false&prepStmtCacheSize=2&useServerPrepStmts=true&credentialType=ENV&sessionVariables=blabla&connectionAttributes=bla=bla&servicePrincipalName=SPN&blankTableNameMeta=true&tinyInt1isBit=false&yearIsDateType=false&dumpQueriesOnException=true&includeInnodbStatusInDeadlockExceptions=true&includeThreadDumpInDeadlockExceptions=true&retriesAllDown=10&galeraAllowedState=A,B&transactionReplay=true&pool=true&poolName=myPool&maxPoolSize=16&minPoolSize=12&maxIdleTime=25000&registerJmxPool=false&poolValidMinDelay=260&useResetConnection=true&serverRsaPublicKeyFile=RSAPath&allowPublicKeyRetrieval=true";
    assertEquals(expected, conf.toString());
    assertEquals(expected, conf.toBuilder().build().toString());
  }

  @Test
  public void useCatalogTerm() throws SQLException {
    Configuration conf =
        Configuration.parse("jdbc:mariadb://localhost/test?useCatalogTerm=Catalog");
    assertEquals(conf.useCatalogTerm(), CatalogTerm.UseCatalog);

    conf = Configuration.parse("jdbc:mariadb://localhost/test?useCatalogTerm=Schema");
    assertEquals(conf.useCatalogTerm(), CatalogTerm.UseSchema);

    assertThrows(
        SQLException.class,
        () -> Configuration.parse("jdbc:mariadb://localhost/test?useCatalogTerm=Wrong"));

    conf = Configuration.parse("jdbc:mariadb://localhost/test?databaseTerm=Catalog");
    assertEquals(conf.useCatalogTerm(), CatalogTerm.UseCatalog);

    conf = Configuration.parse("jdbc:mariadb://localhost/test?databaseTerm=Schema");
    assertEquals(conf.useCatalogTerm(), CatalogTerm.UseSchema);
  }

  @Test
  public void equal() throws SQLException {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/test");
    assertEquals(conf, conf);
    assertEquals(Configuration.parse("jdbc:mariadb://localhost/test"), conf);
    assertNotEquals(null, conf);
    assertNotEquals("", conf);
    assertNotEquals(Configuration.parse("jdbc:mariadb://localhost/test2"), conf);
  }

  @Test
  public void useMysqlMetadata() throws SQLException {
    assertTrue(
        new Configuration.Builder()
            .database("DB")
            .useMysqlMetadata(true)
            .build()
            .useMysqlMetadata());

    assertFalse(
        new Configuration.Builder()
            .database("DB")
            .useMysqlMetadata(false)
            .build()
            .useMysqlMetadata());

    assertFalse(
        new Configuration.Builder()
            .database("DB")
            .useMysqlMetadata(null)
            .build()
            .useMysqlMetadata());
  }

  @Test
  public void toConf() throws SQLException {
    assertTrue(
        Configuration.toConf("jdbc:mariadb://localhost/test")
            .startsWith(
                "Configuration:\n"
                    + " * resulting Url : jdbc:mariadb://localhost/test\n"
                    + "Unknown options : None\n"
                    + "\n"
                    + "Non default options : \n"
                    + " * database : test\n"
                    + "\n"
                    + "default options :"));
    assertTrue(
        Configuration.toConf(
                "jdbc:mariadb:loadbalance://host1:3305,address=(host=host2)(port=3307)(type=replica)/db?nonExisting&nonExistingWithValue=tt&user=me&password=***&autocommit=false&createDatabaseIfNotExist=true&")
            .startsWith(
                "Configuration:\n"
                    + " * resulting Url :"
                    + " jdbc:mariadb:loadbalance://address=(host=host1)(port=3305)(type=primary),address=(host=host2)(port=3307)(type=replica)/db?user=me&password=***&nonExisting=&nonExistingWithValue=tt&autocommit=false&createDatabaseIfNotExist=true\n"
                    + "Unknown options : \n"
                    + " * nonExisting : \n"
                    + " * nonExistingWithValue : tt\n"
                    + "\n"
                    + "Non default options : \n"
                    + " * addresses : [address=(host=host1)(port=3305)(type=primary),"
                    + " address=(host=host2)(port=3307)(type=replica)]\n"
                    + " * autocommit : false\n"
                    + " * createDatabaseIfNotExist : true\n"
                    + " * database : db\n"
                    + " * haMode : LOADBALANCE\n"
                    + " * password : ***\n"
                    + " * user : me\n"
                    + "\n"
                    + "default options :\n"
                    + " * allowLocalInfile : true\n"
                    + " * allowMultiQueries : false\n"
                    + " * allowPublicKeyRetrieval : false"));

    assertTrue(
        Configuration.toConf(
                "jdbc:mariadb://localhost/test?user=root&sslMode=verify-ca&serverSslCert=/tmp/t.pem&trustStoreType=JKS&keyStore=/tmp/keystore&keyStorePassword=kspass")
            .startsWith(
                "Configuration:\n"
                    + " * resulting Url :"
                    + " jdbc:mariadb://localhost/test?user=root&sslMode=VERIFY_CA&serverSslCert=/tmp/t.pem&keyStore=/tmp/keystore&keyStorePassword=***&trustStoreType=JKS\n"
                    + "Unknown options : None\n"
                    + "\n"
                    + "Non default options : \n"
                    + " * database : test\n"
                    + " * keyStore : /tmp/keystore\n"
                    + " * keyStorePassword : ***\n"
                    + " * serverSslCert : /tmp/t.pem\n"
                    + " * sslMode : VERIFY_CA\n"
                    + " * trustStoreType : JKS\n"
                    + " * user : root\n"
                    + "\n"
                    + "default options :\n"
                    + " * addresses : [address=(host=localhost)(port=3306)(type=primary)]\n"
                    + " * allowLocalInfile : true"));
  }
}
