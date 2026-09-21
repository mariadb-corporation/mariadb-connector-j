// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbXid;

/** XA RECOVER rows carry server-controlled lengths: they must be validated before use. */
public class MariaDbXidTest {

  private static void assertRejected(int gtridLength, int bqualLength, byte[] data) {
    XAException e =
        assertThrows(
            XAException.class, () -> MariaDbXid.fromRecoverRow(1, gtridLength, bqualLength, data));
    assertEquals(XAException.XAER_RMFAIL, e.errorCode);
  }

  @Test
  public void validRow() throws XAException {
    byte[] data = "abcde".getBytes();
    MariaDbXid xid = MariaDbXid.fromRecoverRow(7, 2, 3, data);
    assertEquals(7, xid.getFormatId());
    assertArrayEquals("ab".getBytes(), xid.getGlobalTransactionId());
    assertArrayEquals("cde".getBytes(), xid.getBranchQualifier());

    // bqual may be empty
    xid = MariaDbXid.fromRecoverRow(1, 2, 0, "ab".getBytes());
    assertArrayEquals("ab".getBytes(), xid.getGlobalTransactionId());
    assertEquals(0, xid.getBranchQualifier().length);

    // maximum sizes allowed by the XA specification
    byte[] max = new byte[Xid.MAXGTRIDSIZE + Xid.MAXBQUALSIZE];
    Arrays.fill(max, (byte) 'x');
    xid = MariaDbXid.fromRecoverRow(1, Xid.MAXGTRIDSIZE, Xid.MAXBQUALSIZE, max);
    assertEquals(Xid.MAXGTRIDSIZE, xid.getGlobalTransactionId().length);
    assertEquals(Xid.MAXBQUALSIZE, xid.getBranchQualifier().length);

    // payload longer than declared lengths: extra bytes are ignored, not an error
    xid = MariaDbXid.fromRecoverRow(1, 1, 1, "abcd".getBytes());
    assertArrayEquals("a".getBytes(), xid.getGlobalTransactionId());
    assertArrayEquals("b".getBytes(), xid.getBranchQualifier());
  }

  @Test
  public void hugeLengthIsRejectedBeforeAllocation() {
    byte[] data = "abcd".getBytes();
    assertRejected(2147483632, 2, data);
    assertRejected(Integer.MAX_VALUE, 2, data);
    assertRejected(2, Integer.MAX_VALUE, data);
    // sum overflows int: must not wrap around into a "valid" value
    assertRejected(Integer.MAX_VALUE, Integer.MAX_VALUE, data);
  }

  @Test
  public void negativeLengthIsRejected() {
    byte[] data = "abcd".getBytes();
    assertRejected(-1, 2, data);
    assertRejected(2, -1, data);
    assertRejected(Integer.MIN_VALUE, 2, data);
  }

  @Test
  public void lengthExceedingSpecBoundIsRejected() {
    byte[] data = new byte[200];
    assertRejected(Xid.MAXGTRIDSIZE + 1, 2, data);
    assertRejected(2, Xid.MAXBQUALSIZE + 1, data);
  }

  @Test
  public void lengthExceedingPayloadIsRejected() {
    assertRejected(64, 0, "ab".getBytes());
    assertRejected(2, 3, "abcd".getBytes());
    assertRejected(1, 0, new byte[0]);
    assertRejected(1, 0, null);
  }

  @Test
  public void nullPayloadWithZeroLengths() throws XAException {
    MariaDbXid xid = MariaDbXid.fromRecoverRow(1, 0, 0, null);
    assertEquals(0, xid.getGlobalTransactionId().length);
    assertEquals(0, xid.getBranchQualifier().length);
  }
}
