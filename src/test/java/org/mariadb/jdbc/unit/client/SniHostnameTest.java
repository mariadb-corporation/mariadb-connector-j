// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.*;

import javax.net.ssl.SNIHostName;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.IPUtility;

@SuppressWarnings("ConstantConditions")
public class SniHostnameTest {

  @Test
  public void normalHostname() {
    String host = IPUtility.stripTrailingDot("mariadb.database.svc.cluster.local");
    SNIHostName sni = new SNIHostName(host);
    assertEquals("mariadb.database.svc.cluster.local", sni.getAsciiName());
  }

  @Test
  public void trailingDotFqdn() {
    String host = IPUtility.stripTrailingDot("mariadb.database.svc.cluster.local.");
    SNIHostName sni = new SNIHostName(host);
    assertEquals("mariadb.database.svc.cluster.local", sni.getAsciiName());
  }

  @Test
  public void trailingDotSimpleHostname() {
    String host = IPUtility.stripTrailingDot("dbhost.");
    SNIHostName sni = new SNIHostName(host);
    assertEquals("dbhost", sni.getAsciiName());
  }

  @Test
  public void rawTrailingDotRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new SNIHostName("mariadb.database.svc.cluster.local."));
  }

  @Test
  public void hostnameWithoutDotUnchanged() {
    assertEquals("localhost", IPUtility.stripTrailingDot("localhost"));
  }

  @Test
  public void nullHostUnchanged() {
    assertNull(IPUtility.stripTrailingDot(null));
  }

  @Test
  public void validHostnameAsSni() {
    assertEquals(
        "mariadb.example.com", IPUtility.toSniHostname("mariadb.example.com").getAsciiName());
    assertEquals(
        "mariadb.example.com", IPUtility.toSniHostname("mariadb.example.com.").getAsciiName());
    assertEquals("dbhost", IPUtility.toSniHostname("dbhost").getAsciiName());
    assertEquals("xn--bcher-kva.example", IPUtility.toSniHostname("bücher.example").getAsciiName());
  }

  @Test
  public void ipLiteralWithoutSni() {
    assertNull(IPUtility.toSniHostname("10.0.0.1"));
    assertNull(IPUtility.toSniHostname("10.0.0.1."));
    assertNull(IPUtility.toSniHostname("::1"));
    assertNull(IPUtility.toSniHostname("fe80::1%1"));
    assertNull(IPUtility.toSniHostname("::ffff:127.0.0.1"));
  }

  @Test
  public void invalidHostnameWithoutSni() {
    assertNull(IPUtility.toSniHostname("my_db"));
    assertNull(IPUtility.toSniHostname("db_1.internal"));
    assertNull(IPUtility.toSniHostname("-db.example.com"));
    assertNull(IPUtility.toSniHostname("db-.example.com"));
    assertNull(IPUtility.toSniHostname("db..example.com"));
  }

  @Test
  public void missingHostWithoutSni() {
    assertNull(IPUtility.toSniHostname(null));
    assertNull(IPUtility.toSniHostname(""));
    assertNull(IPUtility.toSniHostname("."));
  }

  @Test
  public void trailingDotPreservedForDns() throws Exception {
    org.mariadb.jdbc.Configuration conf =
        org.mariadb.jdbc.Configuration.parse(
            "jdbc:mariadb://mariadb.database.svc.cluster.local.:3306/test");
    assertEquals("mariadb.database.svc.cluster.local.", conf.addresses().get(0).host);
  }
}
