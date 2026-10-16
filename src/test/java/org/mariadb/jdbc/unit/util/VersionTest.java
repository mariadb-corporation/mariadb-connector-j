// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.Version;

@SuppressWarnings("ConstantConditions")
public class VersionTest {

  @Test
  public void testValue() {
    Version v = new Version("3.0.0-alpha-SNAPSHOT");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(0, v.getPatchVersion());
    assertEquals("-alpha-SNAPSHOT", v.getQualifier());

    v = new Version("3.0.0=alpha-SNAPSHOT");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(0, v.getPatchVersion());
    assertEquals("=alpha-SNAPSHOT", v.getQualifier());

    v = new Version("3.0.1");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(1, v.getPatchVersion());
    assertEquals("", v.getQualifier());
  }

  @Test
  public void serverVersion() {
    Version ver = new Version("10.5.2", true);
    assertEquals(10, ver.getMajorVersion());
    assertEquals(5, ver.getMinorVersion());
    assertEquals(2, ver.getPatchVersion());
    assertEquals("10.5.2", ver.getVersion());
    assertTrue(ver.isMariaDBServer());
    assertTrue(ver.versionGreaterOrEqual(10, 5, 1));
    assertTrue(ver.versionGreaterOrEqual(10, 4, 5));
    assertTrue(ver.versionGreaterOrEqual(5, 6, 5));
    assertTrue(ver.versionGreaterOrEqual(10, 5, 2));
    assertFalse(ver.versionGreaterOrEqual(10, 5, 3));
    assertFalse(ver.versionGreaterOrEqual(10, 6, 0));
    assertFalse(ver.versionGreaterOrEqual(11, 0, 0));

    ver = new Version("10.5.2-MariaDB", true);
    assertEquals("-MariaDB", ver.getQualifier());
    assertTrue(ver.isMariaDBServer());

    ver = new Version("8.0.12-something", false);
    assertEquals(8, ver.getMajorVersion());
    assertEquals(0, ver.getMinorVersion());
    assertEquals(12, ver.getPatchVersion());
    assertFalse(ver.isMariaDBServer());

    // the driver's own version is not a server version
    assertFalse(new Version("4.0.0").isMariaDBServer());
  }
}
