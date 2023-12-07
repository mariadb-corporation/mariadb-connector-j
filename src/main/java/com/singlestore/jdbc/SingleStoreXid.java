// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc;

import java.util.Arrays;
import java.util.Objects;
import javax.transaction.xa.Xid;

public class SingleStoreXid implements Xid {

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
  public SingleStoreXid(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
    this.formatId = formatId;
    this.globalTransactionId = globalTransactionId;
    this.branchQualifier = branchQualifier;
  }

  /**
   * Equal implementation.
   *
   * @param obj object to compare
   * @return true if object is SingleStoreXi and as same parameters
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

  public int getFormatId() {
    return formatId;
  }

  public byte[] getGlobalTransactionId() {
    return globalTransactionId;
  }

  public byte[] getBranchQualifier() {
    return branchQualifier;
  }
}
