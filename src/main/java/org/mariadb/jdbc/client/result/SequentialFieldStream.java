// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.result;

import java.io.IOException;
import java.io.InputStream;

/**
 * Stream over the bytes of one field value of the current {@link SequentialResult} row, read
 * directly from the socket: the result-set has read the field length prefix, this stream hands out
 * the following bytes and stops at the end of the field. It is invalidated as soon as the
 * result-set moves to another field or row, or is closed: remaining bytes are then skipped by the
 * result-set.
 *
 * <p>{@link SequentialBlob}, {@link SequentialClob} and the character reader are wrappers over it.
 */
public final class SequentialFieldStream extends InputStream {

  private final SequentialResult result;
  private long remaining;
  private boolean detached;

  SequentialFieldStream(SequentialResult result, long length) {
    this.result = result;
    this.remaining = length;
  }

  /**
   * Number of bytes of the field not yet read.
   *
   * @return remaining bytes
   */
  public long remaining() {
    return remaining;
  }

  void detach() {
    detached = true;
  }

  private void checkDetached() throws IOException {
    if (detached) {
      throw new IOException(
          "Stream is not readable anymore: result-set has moved to another column or row, or is"
              + " closed");
    }
  }

  @Override
  public int read() throws IOException {
    checkDetached();
    if (remaining <= 0) return -1;
    int b = result.readStreamByte(this);
    remaining--;
    return b;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkDetached();
    if (len == 0) return 0;
    if (remaining <= 0) return -1;
    int n = result.readStream(this, b, off, (int) Math.min(len, remaining));
    remaining -= n;
    return n;
  }

  @Override
  public long skip(long n) throws IOException {
    checkDetached();
    long k = Math.min(n, remaining);
    if (k <= 0) return 0;
    result.skipStream(this, k);
    remaining -= k;
    return k;
  }

  @Override
  public int available() {
    return (int) Math.min(remaining, Integer.MAX_VALUE);
  }

  /**
   * Closing the stream does not consume it: the result-set skips remaining bytes when moving on.
   */
  @Override
  public void close() {}
}
