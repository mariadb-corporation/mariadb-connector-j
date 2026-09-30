// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.client.impl.StandardClient;

class RedirectUrlPasswordMaskTest {

  @Test
  void maskPasswordInRedirectUrl() {
    String masked = StandardClient.hidePassword("mariadb://bob:s3cr3t@db2:3307/app?foo=bar");
    assertEquals("mariadb://bob:***@db2:3307/app?foo=bar", masked);
    assertFalse(masked.contains("s3cr3t"));

    // password containing ':' must still be fully masked
    assertEquals(
        "mysql://bob:***@db2/app", StandardClient.hidePassword("mysql://bob:pa:ss@db2/app"));
  }

  @Test
  void maskPasswordContainingAtSign() {
    // REDIRECT_PATTERN accepts '@' in the password: masking must reach the last '@' before host
    assertEquals(
        "mariadb://bob:***@db2/app", StandardClient.hidePassword("mariadb://bob:p@ssword@db2/app"));
    assertEquals(
        "mariadb://bob:***@db2:3307/app?foo=bar",
        StandardClient.hidePassword("mariadb://bob:p@ss@w@rd@db2:3307/app?foo=bar"));
    String masked = StandardClient.hidePassword("mariadb://svc:p@ss:w@rd?x@backend/app");
    assertEquals("mariadb://svc:***@backend/app", masked);
    assertFalse(masked.contains("rd"));
  }

  @Test
  void leaveUrlWithoutPasswordUntouched() {
    assertEquals("mariadb://db2:3307/app", StandardClient.hidePassword("mariadb://db2:3307/app"));
    assertEquals("mariadb://bob@db2/app", StandardClient.hidePassword("mariadb://bob@db2/app"));
    assertNull(StandardClient.hidePassword(null));
    // '@' inside a query value is not a userinfo separator
    assertEquals(
        "mariadb://db2:3307/app?mail=a@b",
        StandardClient.hidePassword("mariadb://db2:3307/app?mail=a@b"));
  }
}
