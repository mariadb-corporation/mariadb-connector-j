/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.*;
import org.mariadb.jdbc.util.constants.HaMode;

@SuppressWarnings("ConstantConditions")
public class ConfigurationTest extends Common {

  @Test
  public void testWrongFormat() {
    try {
      Configuration.parse("jdbc:mariadb:/localhost/test");
      fail("Must have throw exception");
    } catch (SQLException ie) {
      assertTrue(ie.getMessage().contains("url parsing error : '//' is not present in the url"));
    }
  }

  @Test
  public void testNoAdditionalPart() throws SQLException {
    assertEquals(null, Configuration.parse("jdbc:mariadb://localhost/").database());
    assertEquals(null, Configuration.parse("jdbc:mariadb://localhost/").user());
    assertEquals(null, Configuration.parse("jdbc:mariadb://localhost").database());
    assertEquals(null, Configuration.parse("jdbc:mariadb://localhost").user());
    assertEquals(null, Configuration.parse("jdbc:mariadb://localhost?").database());
    assertEquals(null, Configuration.parse("jdbc:mariadb://localhost?").user());
  }

  @Test
  public void testDatabaseOnly() throws SQLException {
    assertEquals("DB", Configuration.parse("jdbc:mariadb://localhost/DB").database());
    assertEquals(null, Configuration.parse("jdbc:mariadb://localhost/DB").user());
  }

  @Test
  public void testUrl() throws SQLException {
    Configuration conf =
        new Configuration.Builder()
            .database("DB")
            .addresses("local", 3306, true)
            .haMode(HaMode.REPLICATION)
            .build();
    assertEquals(
        "jdbc:mariadb:replication://address=(host=local)(port=3306)(type=primary)/DB",
        conf.initialUrl());
    assertEquals(
        "jdbc:mariadb:replication://address=(host=local)(port=3306)(type=primary)/DB",
        conf.toString());
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

    assertEquals(
        "jdbc:mariadb:replication://address=(host=local)(port=3306)(type=primary),address=(host=host2)(port=3307)(type=replica)/DB",
        conf.initialUrl());
  }

  @Test
  public void testAcceptsUrl() {
    Driver driver = new Driver();
    assertTrue(driver.acceptsURL("jdbc:mariadb://localhost/test"));
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost/test"));
  }

  @Test
  public void testSslAlias() throws Throwable {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=verify-full");
    assertTrue(SslMode.VERIFY_FULL == conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=verify_full");
    assertTrue(SslMode.VERIFY_FULL == conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=no-verification");
    assertTrue(SslMode.NO_VERIFICATION == conf.sslMode());

    try {
      Configuration.parse("jdbc:mariadb://localhost/test?sslMode=wrong_trust");
      Assertions.fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Wrong argument value 'wrong_trust' for SslMode"));
    }

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode=verify-ca");
    assertTrue(SslMode.VERIFY_CA == conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test");
    assertTrue(SslMode.DISABLE == conf.sslMode());

    conf = Configuration.parse("jdbc:mariadb://localhost/test?sslMode");
    assertTrue(SslMode.VERIFY_FULL == conf.sslMode());
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
    assertEquals(0, conf.validConnectionTimeout());
    assertNull(conf.user());
    assertEquals(0, conf.socketTimeout());
  }

  @Test
  public void testOptionParse() throws Throwable {
    Configuration conf =
        Configuration.parse(
            "jdbc:mariadb://localhost/test?user=root&password=toto&createDB=true"
                + "&autoReconnect=true&validConnectionTimeout=2&connectTimeout=5&socketTimeout=20");
    assertEquals(5, conf.connectTimeout());
    assertEquals(20, conf.socketTimeout());
    assertEquals(2, conf.validConnectionTimeout());
    assertEquals("true", conf.nonMappedOptions().get("createDB"));
    assertEquals("true", conf.nonMappedOptions().get("autoReconnect"));
    assertEquals("root", conf.user());
    assertEquals("toto", conf.password());
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
                + "&validConnectionTimeout=0&connectTimeout=5");
    assertEquals(5, jdbc.connectTimeout());
    assertEquals(0, jdbc.validConnectionTimeout());
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
  public void testOptionParseIntegerNotPossible() throws Throwable {
    assertThrows(
        SQLException.class,
        () ->
            Configuration.parse(
                "jdbc:mariadb://localhost/test?user=root&autoReconnect=true&validConnectionTimeout=-2"
                    + "&connectTimeout=5"));
  }

  @Test()
  public void testJdbcParserSimpleIpv4basic() throws SQLException {
    String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/database";
    Configuration.parse(url);
  }

  @Test
  public void testJdbcParserSimpleIpv4basicError() throws SQLException {
    Configuration Configuration = org.mariadb.jdbc.Configuration.parse(null);
    assertTrue(Configuration == null);
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
  public void testJdbcParserSimpleIpv4Properties() throws SQLException {
    String url = "jdbc:mariadb://master:3306,slave1:3307,slave2:3308/database?autoReconnect=true";

    Properties prop = new Properties();
    prop.setProperty("user", "greg");
    prop.setProperty("password", "pass");

    Configuration conf = org.mariadb.jdbc.Configuration.parse(url, prop);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
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
                  "Optional parameter autoReconnect must be boolean (true/false or 0/1) was \"truee\""));
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
        "jdbc:mariadb://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306,[2001:660:7401:200::edf:bdd7]:3307"
            + "/database?user=greg&password=pass";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(2, conf.addresses().size());
    assertEquals(
        HostAddress.from("2001:0660:7401:0200:0000:0000:0edf:bdd7", 3306, true),
        conf.addresses().get(0));
    assertEquals(
        HostAddress.from("2001:660:7401:200::edf:bdd7", 3307, true), conf.addresses().get(1));
  }

  @Test
  public void testJdbcParserParameter() throws SQLException {
    String url =
        "jdbc:mariadb://address=(type=primary)(port=3306)(host=master1),address=(port=3307)(type=primary)"
            + "(host=master2),address=(type=replica)(host=slave1)(port=3308)/database?user=greg&password=pass";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
    assertEquals(3, conf.addresses().size());
    assertEquals(HostAddress.from("master1", 3306, true), conf.addresses().get(0));
    assertEquals(HostAddress.from("master2", 3307, true), conf.addresses().get(1));
    assertEquals(HostAddress.from("slave1", 3308, false), conf.addresses().get(2));
  }

  @Test
  public void testJdbcParserParameterErrorEqual() throws SQLException {
    String url =
        "jdbc:mariadb://address=(type=)(port=3306)(host=master1),address=(port=3307)(type=primary)"
            + "(host=master2),address=(type=replica)(host=slave1)(port=3308)/database?user=greg&password=pass";
    assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse(url),
        "Invalid connection URL, expected key=value pairs, found (type=)");
    String url2 =
        "jdbc:mariadb://address=(type=wrong)(port=3306)(host=master1),address=(port=3307)(type=primary)"
            + "(host=master2),address=(type=replica)(host=slave1)(port=3308)/database?user=greg&password=pass";
    assertThrowsContains(
        SQLException.class,
        () -> Configuration.parse(url2),
        "Wrong type value (type=wrong) (possible value primary/replica)");
  }

  @Test
  public void testJdbcParserHaModeNone() throws SQLException {
    String url = "jdbc:mariadb://localhost/database";
    Configuration jdbc = Configuration.parse(url);
    assertTrue(jdbc.haMode().equals(HaMode.NONE));
  }

  @Test
  public void testJdbcParserHaModeLoadReplication() throws SQLException {
    String url = "jdbc:mariadb:replication://localhost/database";
    Configuration jdbc = Configuration.parse(url);
    assertTrue(jdbc.haMode().equals(HaMode.REPLICATION));
  }

  @Test
  public void testJdbcParserReplicationParameter() throws SQLException {
    String url =
        "jdbc:mariadb:replication://address=(type=primary)(port=3306)(host=master1),address=(port=3307)"
            + "(type=primary)(host=master2),address=(type=replica)(host=slave1)(port=3308)/database"
            + "?user=greg&password=pass";
    Configuration conf = org.mariadb.jdbc.Configuration.parse(url);
    assertEquals("database", conf.database());
    assertEquals("greg", conf.user());
    assertEquals("pass", conf.password());
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
    assertTrue(jdbc == null);
  }

  /**
   * CONJ-423] driver doesn't accept connection string with "disableMariaDbDriver".
   *
   * @throws SQLException if any exception occur
   */
  @Test
  public void checkDisable() throws SQLException {
    Configuration jdbc = Configuration.parse("jdbc:mysql://localhost/test?disableMariaDbDriver");
    assertTrue(jdbc == null);
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
    assertEquals(SslMode.VERIFY_FULL, jdbc.sslMode());
    assertEquals("pwd2", jdbc.password());
  }

  @Test
  public void builder() throws SQLException {
    Configuration conf =
        new Configuration.Builder()
            .addresses(
                new HostAddress[] {
                  HostAddress.from("host1", 3305, true), HostAddress.from("host2", 3307, false)
                })
            .user("me")
            .password("pwd")
            .database("db")
            .socketFactory("someSocketFactory")
            .connectTimeout(22)
            .pipe("pipeName")
            .localSocket("localSocket")
            .tcpKeepAlive(true)
            .tcpAbortiveClose(true)
            .localSocketAddress("localSocketAddress")
            .socketTimeout(1000)
            .allowMultiQueries(true)
            .rewriteBatchedStatements(true)
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
            .useBulkStmts(false)
            .autocommit(false)
            .includeInnodbStatusInDeadlockExceptions(true)
            .includeThreadDumpInDeadlockExceptions(true)
            .servicePrincipalName("SPN")
            .defaultFetchSize(10)
            .tlsSocketType("TLStype")
            .maxQuerySizeToLog(100)
            .maxAllowedPacket(40000)
            .retriesAllDown(10)
            .assureReadOnly(true)
            .validConnectionTimeout(100)
            .galeraAllowedState("A,B")
            .enabledSslProtocolSuites("TLSv1.2")
            .pinGlobalTxToPhysicalConnection(false)
            .transactionReplay(true)
            .pool(true)
            .poolName("myPool")
            .maxPoolSize(16)
            .minPoolSize(12)
            .maxIdleTime(25000)
            .staticGlobal(true)
            .registerJmxPool(true)
            .poolValidMinDelay(260)
            .useResetConnection(true)
            .useReadAheadInput(false)
            .cachePrepStmts(false)
            .serverSslCert("mycertPath")
            .serverRsaPublicKeyFile("RSAPath")
            .allowPublicKeyRetrieval(true)
            .build();
    assertEquals(
        "jdbc:mariadb://address=(host=host1)(port=3305)(type=primary),address=(host=host2)(port=3307)(type=replica)/db?user=me&password=pwd&timezone=UTC&autocommit=false&pinGlobalTxToPhysicalConnection=false&socketFactory=someSocketFactory&pipe=pipeName&localSocket=localSocket&localSocketAddress=localSocketAddress&useReadAheadInput=false&tlsSocketType=TLStype&sslMode=REQUIRED&serverSslCert=mycertPath&enabledSslCipherSuites=myCipher,cipher2&enabledSslProtocolSuites=TLSv1.2&useBulkStmts=false&cachePrepStmts=false&credentialType=ENV&sessionVariables=blabla&connectionAttributes=bla=bla&servicePrincipalName=SPN&tinyInt1isBit=false&yearIsDateType=false&galeraAllowedState=A,B&poolName=myPool&serverRsaPublicKeyFile=RSAPath",
        conf.toString());
  }
}
