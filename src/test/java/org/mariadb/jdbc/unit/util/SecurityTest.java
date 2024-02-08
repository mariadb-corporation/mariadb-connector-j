// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.util.Security;

@SuppressWarnings("ConstantConditions")
public class SecurityTest {

  @Test
  public void parser() {
    assertEquals(
        "wait_timeout=5,some='bla;'", Security.parseSessionVariables("wait_timeout=5,some='bla;'"));
    assertEquals(
        "wait_timeout=5,some='bla\"\\n=;'",
        Security.parseSessionVariables("wait_timeout=5;some='bla\"\\n=;'"));
    assertEquals(
        "wait_timeout=5,some=\"bla;'\"",
        Security.parseSessionVariables("wait_timeout=5;some=\"bla;'\""));
    assertEquals("wait_timeout", Security.parseSessionVariables("wait_timeout"));
    assertEquals("wait_timeout=", Security.parseSessionVariables("wait_timeout="));
    assertEquals("wait_timeout=1,t=", Security.parseSessionVariables("wait_timeout=1,t="));
  }
}
