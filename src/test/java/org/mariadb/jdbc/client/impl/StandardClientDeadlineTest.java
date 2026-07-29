// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class StandardClientDeadlineTest {

  @Test
  public void exhaustedBudgetIsDetected() {
    long passed = System.nanoTime() - 1_000_000L;
    assertTrue(StandardClient.connectDeadlineReached(passed, 2_500));
  }

  @Test
  public void remainingBudgetIsPermitted() {
    long future = System.nanoTime() + 60_000L * 1_000_000L;
    assertFalse(StandardClient.connectDeadlineReached(future, 2_500));
  }

  @Test
  public void noDeadlineWhenConnectTimeoutDisabled() {
    long passed = System.nanoTime() - 60_000L * 1_000_000L;
    assertFalse(StandardClient.connectDeadlineReached(passed, 0));
  }
}
