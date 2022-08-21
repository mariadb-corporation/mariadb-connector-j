// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.tidb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.tidb.jdbc.util.Version;

@SuppressWarnings("ConstantConditions")
public class VersionTest {

  @Test
  public void testValue() {
    Version v = new Version("5.7.3-TiDB-3.0.0-beta");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(0, v.getPatchVersion());
    assertEquals("-beta", v.getQualifier());

    v = new Version("5.7.3-TiDB-3.0.0=alpha-SNAPSHOT");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(0, v.getPatchVersion());
    assertEquals("=alpha-SNAPSHOT", v.getQualifier());

    v = new Version("5.7.3-TiDB-3.0.1");
    assertEquals(3, v.getMajorVersion());
    assertEquals(0, v.getMinorVersion());
    assertEquals(1, v.getPatchVersion());
    assertEquals("", v.getQualifier());
  }
}
