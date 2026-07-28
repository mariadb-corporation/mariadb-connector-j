// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.*;

import javax.net.ssl.SNIHostName;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.IPUtility;

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
  public void trailingDotPreservedForDns() throws Exception {
    org.mariadb.jdbc.Configuration conf =
        org.mariadb.jdbc.Configuration.parse(
            "jdbc:mariadb://mariadb.database.svc.cluster.local.:3306/test");
    assertEquals("mariadb.database.svc.cluster.local.", conf.addresses().get(0).host);
  }
}
