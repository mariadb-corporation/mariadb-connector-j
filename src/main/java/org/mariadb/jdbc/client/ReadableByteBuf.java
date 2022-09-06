// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client;

import org.mariadb.jdbc.MariaDbBlob;

/** Packet buffer interface */
public interface ReadableByteBuf {

  /**
   * buffer number of unread bytes
   *
   * @return remaining bytes number
   */
  int readableBytes();

  /**
   * Current buffer position
   *
   * @return position
   */
  int pos();

  /**
   * buffer
   *
   * @return buffer
   */
  byte[] buf();

  /**
   * Reset buffer
   *
   * @param buf new buffer
   * @param limit buffer limit
   * @param pos initial position
   */
  void buf(byte[] buf, int limit, int pos);

  /**
   * Set position
   *
   * @param pos new position
   */
  void pos(int pos);

  /** Skip one byte */
  void skip();

  /**
   * Skip length value of bytes
   *
   * @param length number of position to skip
   */
  void skip(int length);

  /** Skip length encoded value */
  void skipLengthEncoded();

  /**
   * Read Blob at current position
   *
   * @param length blob length
   * @return Blob
   */
  MariaDbBlob readBlob(int length);

  /**
   * Read byte from buffer at current position, without changing position
   *
   * @return byte value
   */
  byte getByte();

  /**
   * Read byte from buffer at indicated index, without changing position
   *
   * @param index index
   * @return byte value
   */
  byte getByte(int index);

  /**
   * Read unsigned byte value at current position, without changing position
   *
   * @return short value
   */
  short getUnsignedByte();

  /**
   * Read encoded length value that cannot be null see
   * https://mariadb.com/kb/en/protocol-data-types/#length-encoded-integers
   *
   * @return encoded length
   */
  long readLongLengthEncodedNotNull();

  /**
   * Read encoded length value that cannot be null see
   * https://mariadb.com/kb/en/protocol-data-types/#length-encoded-integers
   *
   * <p>this is readLongLengthEncodedNotNull limited to 32 bits
   *
   * @return encoded length
   */
  int readIntLengthEncodedNotNull();

  /**
   * Utility to skip length encoded string, returning initial position
   *
   * @return initial position
   */
  int skipIdentifier();

  /**
   * Fast long from text parsing
   *
   * @param length data length
   * @return long value
   */
  long atoi(int length);

  /**
   * Read encoded length value see
   * https://mariadb.com/kb/en/protocol-data-types/#length-encoded-integers
   *
   * @return encoded length
   */
  Integer readLength();

  /**
   * Read byte at current position, incrementing position
   *
   * @return byte at current position
   */
  byte readByte();

  /**
   * Read unsigned byte value at current position
   *
   * @return short value
   */
  short readUnsignedByte();

  /**
   * Read signed 2 bytes value (little endian) at current position
   *
   * @return short value
   */
  short readShort();

  /**
   * Read unsigned 2 bytes value (little endian) at current position
   *
   * @return short value
   */
  int readUnsignedShort();

  /**
   * Read signed 3 bytes value (little endian) at current position
   *
   * @return int value
   */
  int readMedium();

  /**
   * Read unsigned 3 bytes value (little endian) at current position
   *
   * @return int value
   */
  int readUnsignedMedium();

  /**
   * Read signed 4 bytes value (little endian) at current position
   *
   * @return int value
   */
  int readInt();

  /**
   * Read signed 4 bytes value (big endian) at current position
   *
   * @return int value
   */
  int readIntBE();

  /**
   * Read unsigned 4 bytes value (little endian) at current position
   *
   * @return long value
   */
  long readUnsignedInt();

  /**
   * Read signed 8 bytes value (little endian) at current position
   *
   * @return long value
   */
  long readLong();

  /**
   * Read unsigned 4 bytes value (big endian) at current position
   *
   * @return long value
   */
  long readLongBE();

  /**
   * Read as many bytes to fill destination array
   *
   * @param dst destination array
   */
  void readBytes(byte[] dst);

  /**
   * Read null-ended encoded bytes. 0x00 null value won't be in return byte, so position is
   * incremented to returned byte array length + 1
   *
   * @return byte array
   */
  byte[] readBytesNullEnd();

  /**
   * Return a length encoded buffer
   *
   * @return new buffer
   */
  ReadableByteBuf readLengthBuffer();

  /**
   * Read utf-8 encoded string from length bytes
   *
   * @param length length byte to read
   * @return string value
   */
  String readString(int length);

  /**
   * Read ascii encoded string from length bytes
   *
   * @param length length byte to read
   * @return string value
   */
  String readAscii(int length);

  /**
   * Read null-ended utf-8 encoded string. 0x00 = null represent string ending. Position is
   * incremented to returned string corresponding bytes + 1
   *
   * @return corresponding string
   */
  String readStringNullEnd();

  /**
   * Return the utf-8 string represented by current position to the limit of buffer
   *
   * @return string value
   */
  String readStringEof();

  /**
   * Read float encoded on 4 bytes value at current position
   *
   * @return float value
   */
  float readFloat();

  /**
   * Read double encoded on 8 bytes value at current position
   *
   * @return double value
   */
  double readDouble();

  /**
   * Read double encoded on 8 bytes (big endian) value at current position
   *
   * @return double value
   */
  double readDoubleBE();
}
