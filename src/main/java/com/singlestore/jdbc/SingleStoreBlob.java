// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc;

import java.io.*;
import java.sql.Blob;

public class SingleStoreBlob extends BlobImpl implements Blob, Serializable {

  private static final long serialVersionUID = -4736603161284649490L;

  /** Creates an empty Blob. */
  public SingleStoreBlob() {
    super();
  }

  /**
   * Creates a Blob with content.
   *
   * @param bytes the content for the Blob.
   */
  public SingleStoreBlob(byte[] bytes) {
    super(bytes);
  }

  /**
   * Creates a Blob with content.
   *
   * @param bytes the content for the Blob.
   * @param offset offset
   * @param length length
   */
  public SingleStoreBlob(byte[] bytes, int offset, int length) {
    super(bytes, offset, length);
  }

  private SingleStoreBlob(int offset, int length, byte[] bytes) {
    this.data = bytes;
    this.offset = offset;
    this.length = length;
  }

  public static SingleStoreBlob safeSingleStoreBlob(byte[] bytes, int offset, int length) {
    return new SingleStoreBlob(offset, length, bytes);
  }
}
