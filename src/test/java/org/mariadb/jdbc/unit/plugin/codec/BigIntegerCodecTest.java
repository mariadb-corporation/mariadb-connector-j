// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.plugin.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.sql.SQLDataException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.plugin.codec.BigDecimalCodec;
import org.mariadb.jdbc.plugin.codec.BigIntegerCodec;

/**
 * Mirror of {@link BigDecimalCodecTest} for {@link BigIntegerCodec#parseBigInteger(String)}. Both
 * helpers share the same cap; the trap they guard against is {@code new BigInteger(String)} having
 * the same O(n²) parsing cost as {@code new BigDecimal(String)}.
 */
public class BigIntegerCodecTest {

  @Test
  public void parseAtCapAccepted() throws SQLDataException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BigDecimalCodec.MAX_BIG_DECIMAL_STRING_LENGTH; i++) sb.append('9');
    BigInteger parsed = BigIntegerCodec.parseBigInteger(sb.toString());
    assertEquals(sb.toString(), parsed.toString());
  }

  @Test
  public void parseOneOverCapRejected() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BigDecimalCodec.MAX_BIG_DECIMAL_STRING_LENGTH + 1; i++) sb.append('9');
    SQLDataException thrown =
        assertThrows(SQLDataException.class, () -> BigIntegerCodec.parseBigInteger(sb.toString()));
    assertTrue(thrown.getMessage().contains("exceeds"));
  }

  @Test
  public void parseAttackPayloadRejectedQuickly() {
    StringBuilder sb = new StringBuilder(1_000_000);
    for (int i = 0; i < 1_000_000; i++) sb.append('1');
    long start = System.nanoTime();
    SQLDataException thrown =
        assertThrows(SQLDataException.class, () -> BigIntegerCodec.parseBigInteger(sb.toString()));
    long elapsedNanos = System.nanoTime() - start;
    assertTrue(thrown.getMessage().contains("1000000"));
    assertTrue(
        elapsedNanos < 100_000_000L,
        "expected rejection in <100ms, took " + (elapsedNanos / 1_000_000) + "ms");
  }

  @Test
  public void parseValidIntegerRoundtrips() throws SQLDataException {
    assertEquals(BigInteger.ZERO, BigIntegerCodec.parseBigInteger("0"));
    assertEquals(
        new BigInteger("123456789012345"), BigIntegerCodec.parseBigInteger("123456789012345"));
    assertEquals(new BigInteger("-42"), BigIntegerCodec.parseBigInteger("-42"));
  }

  @Test
  public void parseInvalidIntegerStillThrowsNumberFormat() {
    assertThrows(
        NumberFormatException.class, () -> BigIntegerCodec.parseBigInteger("not-a-number"));
  }
}
