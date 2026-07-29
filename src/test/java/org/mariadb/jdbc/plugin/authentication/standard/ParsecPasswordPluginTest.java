// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.standard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ParsecPasswordPluginTest {

  /** conservative reference throughput used by the plugin: 262144 PBKDF2 rounds in 225ms */
  private static final long ROUNDS_PER_MS = 262_144L / 225L;

  @Test
  public void maxIterationFactorFollowsConnectTimeout() {
    assertEquals(6, ParsecPasswordPlugin.maxIterationFactor(100));
    assertEquals(11, ParsecPasswordPlugin.maxIterationFactor(2_500));
    assertEquals(13, ParsecPasswordPlugin.maxIterationFactor(10_000));
    assertEquals(15, ParsecPasswordPlugin.maxIterationFactor(30_000));
  }

  @Test
  public void disabledConnectTimeoutUsesDefaultBudget() {
    assertEquals(10_000, ParsecPasswordPlugin.connectBudgetMs(0));
    assertEquals(10_000, ParsecPasswordPlugin.connectBudgetMs(-1));
    assertEquals(2_500, ParsecPasswordPlugin.connectBudgetMs(2_500));
    assertEquals(
        13, ParsecPasswordPlugin.maxIterationFactor(ParsecPasswordPlugin.connectBudgetMs(0)));
  }

  @Test
  public void maxIterationFactorStaysWithinBounds() {
    // never negative, whatever how small the budget is
    assertEquals(0, ParsecPasswordPlugin.maxIterationFactor(1));
    assertEquals(0, ParsecPasswordPlugin.maxIterationFactor(0));
    // never above the 1024 << factor overflow ceiling, whatever how large the budget is
    assertEquals(20, ParsecPasswordPlugin.maxIterationFactor(Integer.MAX_VALUE));
    assertEquals(20, ParsecPasswordPlugin.maxIterationFactor(3_600_000));
  }

  @Test
  public void hashingAtCapFitsBudgetAndIsMaximal() {
    for (int budget : new int[] {100, 500, 2_500, 10_000, 30_000, 120_000}) {
      int factor = ParsecPasswordPlugin.maxIterationFactor(budget);
      long costMs = (1024L << factor) / ROUNDS_PER_MS;
      assertTrue(
          costMs <= budget, "factor " + factor + " costs " + costMs + "ms > " + budget + "ms");
      if (factor < 20) {
        long nextCostMs = (1024L << (factor + 1)) / ROUNDS_PER_MS;
        assertTrue(
            nextCostMs > budget,
            "factor "
                + (factor + 1)
                + " costs "
                + nextCostMs
                + "ms, should not fit "
                + budget
                + "ms");
      }
    }
  }
}
