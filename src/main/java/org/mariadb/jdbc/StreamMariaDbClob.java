// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.io.*;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

/**
 * MariaDB Clob implementation that uses InputStream to avoid loading the whole data in memory. This
 * implementation ensures that data can only be read once.
 */
public class StreamMariaDbClob extends StreamMariaDbBlob implements Clob, NClob, Serializable {

  private static final long serialVersionUID = -3066501059817815286L;

  /**
   * Creates a new StreamMariaDbClob with the given input stream and initial buffer.
   *
   * @param inputStream the input stream containing the clob data
   * @param initialBuffer the initial buffer containing the first part of the clob data
   * @param initialBufferLength the length of valid data in the initial buffer
   * @param remainingLength the length of remaining data in the input stream
   */
  public StreamMariaDbClob(
      byte[] initialBuffer, int initialBufferLength, InputStream inputStream, int remainingLength) {
    super(initialBuffer, initialBufferLength, inputStream, remainingLength);
  }

  /**
   * ToString implementation.
   *
   * @return string value of clob content.
   */
  public String toString() {
    try {
      return new String(getBytes(1, (int) length()), StandardCharsets.UTF_8);
    } catch (SQLException e) {
      throw new UncheckedIOException("Error reading clob data", new IOException(e));
    }
  }

  /**
   * Get sub string.
   *
   * @param pos position
   * @param length length of sub string
   * @return substring
   * @throws SQLException if pos is less than 1 or length is less than 0
   */
  public String getSubString(long pos, int length) throws SQLException {
    if (pos < 1) {
      throw new SQLException("position must be >= 1");
    }

    if (length < 0) {
      throw new SQLException("length must be > 0");
    }

    String val = toString();
    return val.substring((int) pos - 1, Math.min((int) pos - 1 + length, val.length()));
  }

  public Reader getCharacterStream() throws SQLException {
    return new StringReader(toString());
  }

  /**
   * Returns a Reader object that contains a partial Clob value, starting with the character
   * specified by pos, which is length characters in length.
   *
   * @param pos the offset to the first character of the partial value to be retrieved. The first
   *     character in the Clob is at position 1.
   * @param length the length in characters of the partial value to be retrieved.
   * @return Reader through which the partial Clob value can be read.
   * @throws SQLException if pos is less than 1 or if pos is greater than the number of characters
   *     in the Clob or if pos + length is greater than the number of characters in the Clob
   */
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    String val = toString();
    if (val.length() < (int) pos - 1 + length) {
      throw new SQLException("pos + length is greater than the number of characters in the Clob");
    }
    String sub = val.substring((int) pos - 1, (int) pos - 1 + (int) length);
    return new StringReader(sub);
  }

  /**
   * Set character stream.
   *
   * @param pos position
   * @return writer
   * @throws SQLException if position is invalid
   */
  public Writer setCharacterStream(long pos) throws SQLException {
    throw new SQLException("StreamMariaDbClob is read-only");
  }

  public InputStream getAsciiStream() throws SQLException {
    return getBinaryStream();
  }

  public long position(String searchStr, long start) throws SQLException {
    return toString().indexOf(searchStr, (int) start - 1) + 1;
  }

  public long position(Clob searchStr, long start) throws SQLException {
    return position(searchStr.toString(), start);
  }

  /**
   * Set String.
   *
   * @param pos position
   * @param str string
   * @return string length
   * @throws SQLException if UTF-8 conversion failed
   */
  public int setString(long pos, String str) throws SQLException {
    throw new SQLException("StreamMariaDbClob is read-only");
  }

  public int setString(long pos, String str, int offset, int len) throws SQLException {
    throw new SQLException("StreamMariaDbClob is read-only");
  }

  public OutputStream setAsciiStream(long pos) throws SQLException {
    throw new SQLException("StreamMariaDbClob is read-only");
  }

  /** Return character length of the Clob. Assume UTF8 encoding. */
  @Override
  public long length() throws SQLException {
    // The length of a character string is the number of UTF-16 units (not the number of characters)
    byte[] bytes = getBytes(1, (int) super.length());
    long len = 0;
    int pos = 0;

    // set ASCII (<= 127 chars)
    while (len < bytes.length && bytes[pos] > 0) {
      len++;
      pos++;
    }

    // multi-bytes UTF-8
    while (pos < bytes.length) {
      byte firstByte = bytes[pos++];
      if (firstByte < 0) {
        if (firstByte >> 5 != -2 || (firstByte & 30) == 0) {
          if (firstByte >> 4 == -2) {
            if (pos + 1 < bytes.length) {
              pos += 2;
              len++;
            } else {
              throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
            }
          } else if (firstByte >> 3 != -2) {
            throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
          } else if (pos + 2 < bytes.length) {
            pos += 3;
            len += 2;
          } else {
            // bad truncated UTF8
            pos += bytes.length;
            len += 1;
          }
        } else {
          pos++;
          len++;
        }
      } else {
        len++;
      }
    }
    return len;
  }

  @Override
  public void truncate(final long truncateLen) throws SQLException {
    throw new SQLException("StreamMariaDbClob is read-only");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    StreamMariaDbClob that = (StreamMariaDbClob) o;

    try {
      return toString().equals(that.toString());
    } catch (Exception e) {
      return false;
    }
  }
} 