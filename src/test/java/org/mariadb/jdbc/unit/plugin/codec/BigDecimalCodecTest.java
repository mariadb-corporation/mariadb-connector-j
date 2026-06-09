// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.plugin.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.plugin.codec.BigDecimalCodec;

/**
 * Regression test for the CPU-exhaustion issue: attacker-controlled long-digit strings reaching
 * {@code new BigDecimal(String)} cause O(n²) parsing inside the JDK constructor. {@link
 * BigDecimalCodec#parseBigDecimal(String)} caps input at {@link
 * BigDecimalCodec#MAX_BIG_DECIMAL_STRING_LENGTH} characters and rejects longer input with a {@link
 * SQLDataException}.
 */
public class BigDecimalCodecTest {

  @Test
  public void parseAtCapAccepted() throws SQLDataException {
    // A 65-digit decimal (exactly the cap) round-trips correctly.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BigDecimalCodec.MAX_BIG_DECIMAL_STRING_LENGTH; i++) sb.append('9');
    BigDecimal parsed = BigDecimalCodec.parseBigDecimal(sb.toString());
    assertEquals(sb.toString(), parsed.toPlainString());
  }

  @Test
  public void parseOneOverCapRejected() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BigDecimalCodec.MAX_BIG_DECIMAL_STRING_LENGTH + 1; i++) sb.append('9');
    SQLDataException thrown =
        assertThrows(SQLDataException.class, () -> BigDecimalCodec.parseBigDecimal(sb.toString()));
    assertTrue(thrown.getMessage().contains("exceeds"));
  }

  @Test
  public void parseAttackPayloadRejectedQuickly() {
    // A 1,000,000-digit payload would hold a worker thread ~24s under the unguarded path.
    // The cap check is a String.length() comparison — sub-millisecond on any JVM.
    StringBuilder sb = new StringBuilder(1_000_000);
    for (int i = 0; i < 1_000_000; i++) sb.append('1');
    long start = System.nanoTime();
    SQLDataException thrown =
        assertThrows(SQLDataException.class, () -> BigDecimalCodec.parseBigDecimal(sb.toString()));
    long elapsedNanos = System.nanoTime() - start;
    assertTrue(thrown.getMessage().contains("1000000"));
    // Cap rejection should be effectively instant — generous 100 ms ceiling to avoid flakes on
    // very slow CI hosts. Unguarded BigDecimal(String) would take seconds.
    assertTrue(
        elapsedNanos < 100_000_000L,
        "expected rejection in <100ms, took " + (elapsedNanos / 1_000_000) + "ms");
  }

  @Test
  public void parseValidNumberRoundtrips() throws SQLDataException {
    assertEquals(BigDecimal.ZERO, BigDecimalCodec.parseBigDecimal("0"));
    assertEquals(
        new BigDecimal("3.14159265358979"), BigDecimalCodec.parseBigDecimal("3.14159265358979"));
    assertEquals(new BigDecimal("-1e10"), BigDecimalCodec.parseBigDecimal("-1e10"));
  }

  @Test
  public void parseInvalidNumberFormatStillThrowsNumberFormat() {
    // Below-cap garbage propagates as NumberFormatException (callers wrap with column context).
    assertThrows(
        NumberFormatException.class, () -> BigDecimalCodec.parseBigDecimal("not-a-number"));
  }
}
