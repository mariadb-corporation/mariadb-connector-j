// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.result;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.mariadb.jdbc.MariaDbClob;

/**
 * Read-only, forward-only {@link Clob} whose UTF-8 content is streamed from the socket by a {@link
 * SequentialResult}: the data is never loaded entirely in memory, except by {@link #length()}.
 *
 * <p>Characters can only be read once and in order: {@link #getSubString(long, int)} and {@link
 * #getCharacterStream(long, long)} accept any position at or after the characters already consumed,
 * and skip up to it. {@link #getCharacterStream()} always returns the same reader. {@link
 * #getAsciiStream()} returns the raw byte stream and must not be mixed with character reads. The
 * clob is invalidated when the result-set moves to another column or row.
 *
 * <p>{@link #length()} needs the number of characters, which is only known once all bytes are read:
 * it loads the remaining content in memory, from which the following reads are served.
 */
public final class SequentialClob implements Clob, NClob {

  private InputStream in;
  private final long byteLength;
  private Utf8StreamReader reader;
  private boolean freed;

  SequentialClob(SequentialFieldStream stream, long byteLength) {
    this.in = stream;
    this.byteLength = byteLength;
  }

  private void checkFreed() throws SQLException {
    if (freed) throw new SQLException("Clob has been freed");
  }

  private Utf8StreamReader reader() {
    if (reader == null) reader = new Utf8StreamReader(in);
    return reader;
  }

  /** Move to the 0-based character position, which must not be before consumed characters. */
  private void forward(long position) throws SQLException {
    Utf8StreamReader r = reader();
    long consumed = r.consumedChars();
    if (position < consumed) {
      throw new SQLException(
          String.format(
              "Sequential Clob: cannot read from position %d, %d characters have already been"
                  + " consumed. Data can only be read once, in order",
              position + 1, consumed));
    }
    try {
      long toSkip = position - consumed;
      while (toSkip > 0) {
        long skipped = r.skip(toSkip);
        if (skipped <= 0) break;
        toSkip -= skipped;
      }
    } catch (IOException e) {
      throw new SQLException("Error while reading Clob data", "08000", e);
    }
  }

  /**
   * Number of characters (UTF-16 units). This requires reading the whole content: remaining bytes
   * are loaded in memory, and following reads are served from memory.
   *
   * @return character length
   * @throws SQLException if data cannot be read
   */
  @Override
  public long length() throws SQLException {
    checkFreed();
    Utf8StreamReader r = reader();
    byte[] all;
    try {
      byte[] pending = r.takePendingBytes();
      byte[] rest = in.readAllBytes();
      if (pending.length == 0) {
        all = rest;
      } else {
        all = new byte[pending.length + rest.length];
        System.arraycopy(pending, 0, all, 0, pending.length);
        System.arraycopy(rest, 0, all, pending.length, rest.length);
      }
    } catch (IOException e) {
      throw new SQLException("Error while reading Clob data", "08000", e);
    }
    in = new ByteArrayInputStream(all);
    r.setInput(in);
    return r.consumedChars() + new MariaDbClob(all).length();
  }

  /**
   * Byte length of the UTF-8 content, as declared by the server.
   *
   * @return byte length
   */
  public long byteLength() {
    return byteLength;
  }

  @Override
  public String getSubString(long pos, int len) throws SQLException {
    checkFreed();
    if (pos < 1) throw new SQLException("Position must be >= 1");
    if (len < 0) throw new SQLException("Length must be >= 0");
    forward(pos - 1);
    char[] buf = new char[len];
    int off = 0;
    try {
      while (off < len) {
        int n = reader().read(buf, off, len - off);
        if (n < 0) break;
        off += n;
      }
    } catch (IOException e) {
      throw new SQLException("Error while reading Clob data", "08000", e);
    }
    return new String(buf, 0, off);
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    checkFreed();
    return reader();
  }

  @Override
  public Reader getCharacterStream(long pos, long len) throws SQLException {
    checkFreed();
    if (pos < 1) throw new SQLException("Position must be >= 1");
    if (len < 0) throw new SQLException("Length must be >= 0");
    forward(pos - 1);
    return new LimitedReader(reader(), len);
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    checkFreed();
    return in;
  }

  @Override
  public long position(String searchStr, long start) throws SQLException {
    throw new SQLFeatureNotSupportedException("position is not supported on a sequential Clob");
  }

  @Override
  public long position(Clob searchStr, long start) throws SQLException {
    throw new SQLFeatureNotSupportedException("position is not supported on a sequential Clob");
  }

  @Override
  public int setString(long pos, String str) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Clob is read-only");
  }

  @Override
  public int setString(long pos, String str, int offset, int len) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Clob is read-only");
  }

  @Override
  public OutputStream setAsciiStream(long pos) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Clob is read-only");
  }

  @Override
  public Writer setCharacterStream(long pos) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Clob is read-only");
  }

  @Override
  public void truncate(long len) throws SQLException {
    throw new SQLFeatureNotSupportedException("Sequential Clob is read-only");
  }

  /** Remaining bytes are skipped by the result-set when it moves on. */
  @Override
  public void free() {
    freed = true;
  }

  /** View limited to a number of characters of the reader. */
  static final class LimitedReader extends Reader {
    private final Reader reader;
    private long remaining;

    LimitedReader(Reader reader, long limit) {
      this.reader = reader;
      this.remaining = limit;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      if (len == 0) return 0;
      if (remaining <= 0) return -1;
      int n = reader.read(cbuf, off, (int) Math.min(len, remaining));
      if (n > 0) remaining -= n;
      return n;
    }

    @Override
    public void close() {}
  }
}
