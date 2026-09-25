// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.result;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Read-only, forward-only {@link Blob} whose content is streamed from the socket by a {@link
 * SequentialResult}: the data is never loaded entirely in memory.
 *
 * <p>Bytes can only be read once and in order: {@link #getBytes(long, int)} and {@link
 * #getBinaryStream(long, long)} accept any position at or after the bytes already consumed, and
 * skip up to it. The blob is invalidated when the result-set moves to another column or row.
 */
public final class SequentialBlob implements Blob {

  private final SequentialFieldStream stream;
  private final long length;
  private boolean freed;

  SequentialBlob(SequentialFieldStream stream, long length) {
    this.stream = stream;
    this.length = length;
  }

  private void checkFreed() throws SQLException {
    if (freed) throw new SQLException("Blob has been freed");
  }

  private long consumed() {
    return length - stream.remaining();
  }

  /** Move to the 0-based position, which must not be before already consumed bytes. */
  private void forward(long position) throws SQLException {
    long consumed = consumed();
    if (position < consumed) {
      throw new SQLException(
          String.format(
              "Sequential Blob: cannot read from position %d, %d bytes have already been consumed."
                  + " Data can only be read once, in order",
              position + 1, consumed));
    }
    try {
      long toSkip = position - consumed;
      while (toSkip > 0) {
        long skipped = stream.skip(toSkip);
        if (skipped <= 0) break;
        toSkip -= skipped;
      }
    } catch (IOException e) {
      throw new SQLException("Error while reading Blob data", "08000", e);
    }
  }

  @Override
  public long length() throws SQLException {
    checkFreed();
    return length;
  }

  @Override
  public byte[] getBytes(long pos, int len) throws SQLException {
    checkFreed();
    if (pos < 1) throw new SQLException("Position must be >= 1");
    if (len < 0) throw new SQLException("Length must be >= 0");
    long start = pos - 1;
    if (start > length) throw new SQLException("Position exceeds Blob length");
    forward(start);
    int toRead = (int) Math.min(len, length - start);
    byte[] out = new byte[toRead];
    try {
      int off = 0;
      while (off < toRead) {
        int n = stream.read(out, off, toRead - off);
        if (n < 0) break;
        off += n;
      }
    } catch (IOException e) {
      throw new SQLException("Error while reading Blob data", "08000", e);
    }
    return out;
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    checkFreed();
    return stream;
  }

  @Override
  public InputStream getBinaryStream(long pos, long len) throws SQLException {
    checkFreed();
    if (pos < 1) throw new SQLException("Position must be >= 1");
    if (len < 0) throw new SQLException("Length must be >= 0");
    if (pos - 1 + len > length) {
      throw new SQLException("Position + length exceeds Blob length");
    }
    forward(pos - 1);
    return new LimitedInputStream(stream, len);
  }

  @Override
  public long position(byte[] pattern, long start) throws SQLException {
    throw new SQLFeatureNotSupportedException("position is not supported on a sequential Blob");
  }

  @Override
  public long position(Blob pattern, long start) throws SQLException {
    throw new SQLFeatureNotSupportedException("position is not supported on a sequential Blob");
  }

  @Override
  public int setBytes(long pos, byte[] bytes) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Blob is read-only");
  }

  @Override
  public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Blob is read-only");
  }

  @Override
  public OutputStream setBinaryStream(long pos) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Blob is read-only");
  }

  @Override
  public void truncate(long len) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Blob is read-only");
  }

  /** Remaining bytes are skipped by the result-set when it moves on. */
  @Override
  public void free() {
    freed = true;
  }

  /** View limited to a number of bytes of the column stream. */
  static final class LimitedInputStream extends InputStream {
    private final InputStream in;
    private long remaining;

    LimitedInputStream(InputStream in, long limit) {
      this.in = in;
      this.remaining = limit;
    }

    @Override
    public int read() throws IOException {
      if (remaining <= 0) return -1;
      int b = in.read();
      if (b >= 0) remaining--;
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) return 0;
      if (remaining <= 0) return -1;
      int n = in.read(b, off, (int) Math.min(len, remaining));
      if (n > 0) remaining -= n;
      return n;
    }

    @Override
    public long skip(long n) throws IOException {
      long k = in.skip(Math.min(n, remaining));
      remaining -= k;
      return k;
    }

    @Override
    public int available() throws IOException {
      return (int) Math.min(in.available(), remaining);
    }
  }
}
