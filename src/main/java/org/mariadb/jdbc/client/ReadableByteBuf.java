// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;

import org.mariadb.jdbc.MariaDbBlob;

/** Packet buffer interface */
public interface ReadableByteBuf {

  /**
   * buffer number of unread bytes
   *
   * @return remaining bytes number
   */
  int readableBytes();

  int pos();
  /**
   * Set position
   *
   * @param pos new position
   */
  void pos(int pos);

  /** Skip one byte */
  void skip() throws IOException;

  void ensureAvailable(int bytes) throws IOException;

  /**
   * Skip length value of bytes
   *
   * @param length number of position to skip
   */
  void skip(int length) throws IOException;

  /** Skip length encoded value */
  void skipLengthEncoded() throws IOException;

  /**
   * Read Blob at current position
   *
   * @param length blob length
   * @return Blob
   */
  Blob readBlob(int length) throws IOException;

  Clob readClob(int length) throws IOException;

  /**
   * Read inputStream at current position
   *
   * @param length blob length
   * @return Blob
   */
  InputStream readInputStream(int length);

  /**
   * Read byte from buffer at current position, without changing position
   *
   * @return byte value
   */
  byte getByte() throws IOException;

  /**
   * Read byte from buffer at indicated index, without changing position
   *
   * @param index index
   * @return byte value
   */
  byte getByte(int index) throws IOException;

  /**
   * Read unsigned byte value at current position, without changing position
   *
   * @return short value
   */
  short getUnsignedByte() throws IOException;

  /**
   * Read encoded length value that cannot be null
   *
   * @see <a href="https://mariadb.com/kb/en/protocol-data-types/#length-encoded-integers">length
   *     encoded integer</a>
   * @return encoded length
   */
  long readLongLengthEncodedNotNull() throws IOException;

  /**
   * Read encoded length value that cannot be null
   *
   * @see <a href="https://mariadb.com/kb/en/protocol-data-types/#length-encoded-integers">length
   *     encoded integer</a>
   *     <p>this is readLongLengthEncodedNotNull limited to 32 bits
   * @return encoded length
   */
  int readIntLengthEncodedNotNull() throws IOException;

  /**
   * Utility to skip length encoded string, returning initial position
   *
   * @return initial position
   */
  int skipIdentifier() throws IOException;

  /**
   * Fast signed long parsing
   *
   * @param length data length
   * @return long value
   */
  long atoll(int length) throws IOException;

  /**
   * Fast unsigned long parsing
   *
   * @param length data length
   * @return long value
   */
  long atoull(int length) throws IOException;

  /**
   * Read encoded length value
   *
   * @see <a href="https://mariadb.com/kb/en/protocol-data-types/#length-encoded-integers">length
   *     encoded integer</a>
   * @return encoded length
   */
  Integer readLength() throws IOException;

  /**
   * Read byte at current position, incrementing position
   *
   * @return byte at current position
   */
  byte readByte() throws IOException;

  /**
   * Read unsigned byte value at current position
   *
   * @return short value
   */
  short readUnsignedByte() throws IOException;

  /**
   * Read signed 2 bytes value (little endian) at current position
   *
   * @return short value
   */
  short readShort() throws IOException;

  /**
   * Read unsigned 2 bytes value (little endian) at current position
   *
   * @return short value
   */
  int readUnsignedShort() throws IOException;

  /**
   * Read signed 3 bytes value (little endian) at current position
   *
   * @return int value
   */
  int readMedium() throws IOException;

  /**
   * Read unsigned 3 bytes value (little endian) at current position
   *
   * @return int value
   */
  int readUnsignedMedium() throws IOException;

  /**
   * Read signed 4 bytes value (little endian) at current position
   *
   * @return int value
   */
  int readInt() throws IOException;

  /**
   * Read signed 4 bytes value (big endian) at current position
   *
   * @return int value
   */
  int readIntBE() throws IOException;

  /**
   * Read unsigned 4 bytes value (little endian) at current position
   *
   * @return long value
   */
  long readUnsignedInt() throws IOException;

  /**
   * Read signed 8 bytes value (little endian) at current position
   *
   * @return long value
   */
  long readLong() throws IOException;

  /**
   * Read unsigned 4 bytes value (big endian) at current position
   *
   * @return long value
   */
  long readLongBE() throws IOException;

  /**
   * Read as many bytes to fill destination array
   *
   * @param dst destination array
   */
  void readBytes(byte[] dst) throws IOException;

  /**
   * Read null-ended encoded bytes. 0x00 null value won't be in return byte, so position is
   * incremented to returned byte array length + 1
   *
   * @return byte array
   */
  byte[] readBytesNullEnd() throws IOException;

  /**
   * Return a length encoded buffer
   *
   * @return new buffer
   */
  ReadableByteBuf readLengthBuffer() throws IOException;

  /**
   * Read utf-8 encoded string from length bytes
   *
   * @param length length byte to read
   * @return string value
   */
  String readString(int length) throws IOException;

  /**
   * Read ascii encoded string from length bytes
   *
   * @param length length byte to read
   * @return string value
   */
  String readAscii(int length) throws IOException;

  /**
   * Read null-ended utf-8 encoded string. 0x00 = null represent string ending. Position is
   * incremented to returned string corresponding bytes + 1
   *
   * @return corresponding string
   */
  String readStringNullEnd() throws IOException;

  /**
   * Return the utf-8 string represented by current position to the limit of buffer
   *
   * @return string value
   */
  String readStringEof() throws IOException;

  /**
   * Read float encoded on 4 bytes value at current position
   *
   * @return float value
   */
  float readFloat() throws IOException;

  /**
   * Read double encoded on 8 bytes value at current position
   *
   * @return double value
   */
  double readDouble() throws IOException;

  /**
   * Read double encoded on 8 bytes (big endian) value at current position
   *
   * @return double value
   */
  double readDoubleBE() throws IOException;
}
