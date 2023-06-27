// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.socket.impl;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Permit to buf socket data, reading not only asked bytes, but available number of bytes when
 * possible.
 */
public class ReadAheadBufferedStream extends FilterInputStream {

  private static final int BUF_SIZE = 16384;
  private final byte[] buf;
  private int end;
  private int pos;

  /**
   * Constructor
   *
   * @param in socket input stream
   */
  public ReadAheadBufferedStream(InputStream in) {
    super(in);
    buf = new byte[BUF_SIZE];
    end = 0;
    pos = 0;
  }

  /**
   * Returning byte array, from cache of reading socket if needed.
   *
   * @param externalBuf buf to fill
   * @param off offset
   * @param len length to read
   * @return number of added bytes
   * @throws IOException if exception during socket reading
   */
  public synchronized int read(byte[] externalBuf, int off, int len) throws IOException {

    if (len == 0) {
      return 0;
    }

    int totalReads = 0;
    while (true) {

      // read
      if (end - pos <= 0) {
        if (len - totalReads >= buf.length) {
          // buf length is less than asked byte and buf is empty
          // => filling directly into external buf
          int reads = super.read(externalBuf, off + totalReads, len - totalReads);
          if (reads <= 0) {
            return (totalReads == 0) ? -1 : totalReads;
          }
          return totalReads + reads;

        } else {

          // filling internal buf
          fillingBuffer(len - totalReads);
          if (end <= 0) {
            return (totalReads == 0) ? -1 : totalReads;
          }
        }
      }

      // copy internal value to buf.
      int copyLength = Math.min(len - totalReads, end - pos);
      System.arraycopy(buf, pos, externalBuf, off + totalReads, copyLength);
      pos += copyLength;
      totalReads += copyLength;

      if (totalReads >= len || super.available() <= 0) {
        return totalReads;
      }
    }
  }

  /**
   * Fill buf with required length, or available bytes.
   *
   * @param minNeededBytes asked number of bytes
   * @throws IOException in case of failing reading stream.
   */
  private void fillingBuffer(int minNeededBytes) throws IOException {
    int lengthToReallyRead = Math.min(BUF_SIZE, Math.max(super.available(), minNeededBytes));
    end = super.read(buf, 0, lengthToReallyRead);
    pos = 0;
  }

  public boolean markSupported() {
    return false;
  }

  public void close() throws IOException {
    super.close();
    end = 0;
    pos = 0;
  }

  public synchronized int available() throws IOException {
    return end - pos + super.available();
  }

  public int read() throws IOException {
    throw new IOException("read() from socket not implemented");
  }

  public long skip(long n) throws IOException {
    throw new IOException("Skip from socket not implemented");
  }

  public void reset() throws IOException {
    throw new IOException("reset from socket not implemented");
  }
}
