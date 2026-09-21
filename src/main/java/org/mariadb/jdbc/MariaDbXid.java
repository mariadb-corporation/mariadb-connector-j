// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc;

import java.util.Arrays;
import java.util.Objects;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

/** MariaDB XID implementation */
public class MariaDbXid implements Xid {

  private final int formatId;
  private final byte[] globalTransactionId;
  private final byte[] branchQualifier;

  /**
   * Global transaction identifier.
   *
   * @param formatId the format identifier part of the XID.
   * @param globalTransactionId the global transaction identifier part of XID as an array of bytes.
   * @param branchQualifier the transaction branch identifier part of XID as an array of bytes.
   */
  public MariaDbXid(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
    this.formatId = formatId;
    this.globalTransactionId = globalTransactionId;
    this.branchQualifier = branchQualifier;
  }

  /**
   * Build an XID from an XA RECOVER row. The row carries the gtrid and bqual concatenated in a
   * single {@code data} column, split according to the {@code gtrid_length} and {@code
   * bqual_length} columns. Those lengths come from the server, so they are validated against the XA
   * specification bounds and the actual payload before any allocation or copy: a rogue or
   * man-in-the-middle server must not be able to drive the client into a huge allocation or an
   * undeclared runtime exception.
   *
   * @param formatId the format identifier part of the XID
   * @param gtridLength server-declared global transaction identifier length
   * @param bqualLength server-declared branch qualifier length
   * @param data concatenated gtrid + bqual bytes
   * @return the parsed XID
   * @throws XAException with XAER_RMFAIL when the lengths are negative, exceed {@link
   *     Xid#MAXGTRIDSIZE}/{@link Xid#MAXBQUALSIZE}, or do not fit the payload
   */
  public static MariaDbXid fromRecoverRow(
      int formatId, int gtridLength, int bqualLength, byte[] data) throws XAException {
    byte[] payload = data == null ? new byte[0] : data;
    int available = payload.length;
    if (gtridLength < 0
        || bqualLength < 0
        || gtridLength > MAXGTRIDSIZE
        || bqualLength > MAXBQUALSIZE
        || (long) gtridLength + (long) bqualLength > available) {
      XAException xaException =
          new XAException(
              "XA RECOVER returned an invalid XID: gtrid_length="
                  + gtridLength
                  + ", bqual_length="
                  + bqualLength
                  + ", data length="
                  + available);
      xaException.errorCode = XAException.XAER_RMFAIL;
      throw xaException;
    }
    byte[] globalTransactionId = Arrays.copyOfRange(payload, 0, gtridLength);
    byte[] branchQualifier = Arrays.copyOfRange(payload, gtridLength, gtridLength + bqualLength);
    return new MariaDbXid(formatId, globalTransactionId, branchQualifier);
  }

  /**
   * Equal implementation.
   *
   * @param obj object to compare
   * @return true if object is MariaDbXi and as same parameters
   */
  public boolean equals(Object obj) {
    if (obj instanceof Xid) {
      Xid other = (Xid) obj;
      return formatId == other.getFormatId()
          && Arrays.equals(globalTransactionId, other.getGlobalTransactionId())
          && Arrays.equals(branchQualifier, other.getBranchQualifier());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(formatId);
    result = 31 * result + Arrays.hashCode(globalTransactionId);
    result = 31 * result + Arrays.hashCode(branchQualifier);
    return result;
  }

  /**
   * Get format id from XID
   *
   * @return format id
   */
  public int getFormatId() {
    return formatId;
  }

  /**
   * Get global transaction id from XID
   *
   * @return global transaction id
   */
  public byte[] getGlobalTransactionId() {
    return globalTransactionId;
  }

  /**
   * Get branch qualifier from XID
   *
   * @return branch qualifier
   */
  public byte[] getBranchQualifier() {
    return branchQualifier;
  }
}
