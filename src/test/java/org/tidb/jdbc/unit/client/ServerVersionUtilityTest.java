// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.tidb.jdbc.message.server.util.ServerVersionUtility;

public class ServerVersionUtilityTest {

  @Test
  public void check() {
    ServerVersionUtility ver = new ServerVersionUtility("5.7.25-TiDB-v6.2.11", true);
    assertEquals(6, ver.getMajorVersion());
    assertEquals(2, ver.getMinorVersion());
    assertEquals(11, ver.getPatchVersion());
    assertEquals("5.7.25-TiDB-v6.2.11", ver.getVersion());
    assertTrue(ver.isTiDBServer());
    assertTrue(ver.versionGreaterOrEqual(6, 2, 10));
    assertTrue(ver.versionGreaterOrEqual(6, 2, 11));
    assertFalse(ver.versionGreaterOrEqual(6, 3, 5));
    assertFalse(ver.versionGreaterOrEqual(7, 1, 1));
    assertFalse(ver.versionGreaterOrEqual(10, 1, 12));
    assertTrue(ver.versionGreaterOrEqual(5, 3, 12));

    ver = new ServerVersionUtility("5.7.25-TiDB-v1.123.1-beta", true);
    assertEquals(1, ver.getMajorVersion());
    assertEquals(123, ver.getMinorVersion());
    assertEquals(1, ver.getPatchVersion());
    assertEquals("5.7.25-TiDB-v1.123.1-beta", ver.getVersion());
    assertEquals("-beta", ver.getQualifier());
    assertTrue(ver.isTiDBServer());

    ver = new ServerVersionUtility("8.0.12-something", false);
    assertFalse(ver.isTiDBServer());
  }
}
