// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab

package org.mariadb.jdbc.client.impl;

import java.nio.charset.StandardCharsets;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.client.ReadableByteBuf;

/** Packet buffer */
public final class StandardReadableByteBuf implements ReadableByteBuf {
  /** buffer */
  public byte[] buf;

  /** current position reading buffer */
  public int pos;

  /** row data limit */
  private int limit;

  /**
   * Packet buffer constructor
   *
   * @param buf buffer
   * @param limit buffer limit
   */
  public StandardReadableByteBuf(byte[] buf, int limit) {
    this.pos = 0;
    this.buf = buf;
    this.limit = limit;
  }

  /**
   * Packet buffer constructor, limit being the buffer length
   *
   * @param buf buffer
   */
  public StandardReadableByteBuf(byte[] buf) {
    this.pos = 0;
    this.buf = buf;
    this.limit = buf.length;
  }

  public int readableBytes() {
    return limit - pos;
  }

  public int pos() {
    return pos;
  }

  public byte[] buf() {
    return buf;
  }

  public void buf(byte[] buf, int limit, int pos) {
    this.buf = buf;
    this.limit = limit;
    this.pos = pos;
  }

  public void pos(int pos) {
    this.pos = pos;
  }

  public void skip() {
    pos++;
  }

  public void skip(int length) {
    pos += length;
  }

  public void skipLengthEncoded() {
    byte len = buf[pos++];
    switch (len) {
      case (byte) 251:
        return;
      case (byte) 252:
        skip(readUnsignedShort());
        return;
      case (byte) 253:
        skip(readUnsignedMedium());
        return;
      case (byte) 254:
        skip((int) (4 + readUnsignedInt()));
        return;
      default:
        pos += len & 0xff;
    }
  }

  public MariaDbBlob readBlob(int length) {
    pos += length;
    return MariaDbBlob.safeMariaDbBlob(buf, pos - length, length);
  }

  public long atoll(int length) {
    boolean negate = false;
    int idx = 0;
    long result = 0;

    if (length > 0 && buf[pos] == 45) { // minus sign
      negate = true;
      pos++;
      idx++;
    }

    while (idx++ < length) {
      result = result * 10 + buf[pos++] - 48;
    }

    return (negate) ? -1 * result : result;
  }

  public long atoull(int length) {
    long result = 0;
    for (int idx = 0; idx < length; idx++) {
      result = result * 10 + buf[pos++] - 48;
    }
    return result;
  }

  public byte getByte() {
    return buf[pos];
  }

  public byte getByte(int index) {
    return buf[index];
  }

  public short getUnsignedByte() {
    return (short) (buf[pos] & 0xff);
  }

  public long readLongLengthEncodedNotNull() {
    int type = (buf[pos++] & 0xff);
    if (type < 251) return type;
    switch (type) {
      case 252: // 0xfc
        return readUnsignedShort();
      case 253: // 0xfd
        return readUnsignedMedium();
      default: // 0xfe
        return readLong();
    }
  }

  public int readIntLengthEncodedNotNull() {
    int type = (buf[pos++] & 0xff);
    if (type < 251) return type;
    switch (type) {
      case 252:
        return readUnsignedShort();
      case 253:
        return readUnsignedMedium();
      case 254:
        return (int) readLong();
      default:
        return type;
    }
  }

  /**
   * Identifier can have a max length of 256 (alias) So no need to check whole length encoding.
   *
   * @return current pos
   */
  public int skipIdentifier() {
    int len = readIntLengthEncodedNotNull();
    pos += len;
    return pos;
  }

  public Integer readLength() {
    int type = readUnsignedByte();
    switch (type) {
      case 251:
        return null;
      case 252:
        return readUnsignedShort();
      case 253:
        return readUnsignedMedium();
      case 254:
        return (int) readLong();
      default:
        return type;
    }
  }

  public byte readByte() {
    return buf[pos++];
  }

  public short readUnsignedByte() {
    return (short) (buf[pos++] & 0xff);
  }

  public short readShort() {
    return (short) ((buf[pos++] & 0xff) + (buf[pos++] << 8));
  }

  public int readUnsignedShort() {
    return ((buf[pos++] & 0xff) + (buf[pos++] << 8)) & 0xffff;
  }

  public int readMedium() {
    int value = readUnsignedMedium();
    if ((value & 0x800000) != 0) {
      value |= 0xff000000;
    }
    return value;
  }

  public int readUnsignedMedium() {
    return ((buf[pos++] & 0xff) + ((buf[pos++] & 0xff) << 8) + ((buf[pos++] & 0xff) << 16));
  }

  public int readInt() {
    return ((buf[pos++] & 0xff)
        + ((buf[pos++] & 0xff) << 8)
        + ((buf[pos++] & 0xff) << 16)
        + ((buf[pos++] & 0xff) << 24));
  }

  public int readIntBE() {
    return (((buf[pos++] & 0xff) << 24)
        + ((buf[pos++] & 0xff) << 16)
        + ((buf[pos++] & 0xff) << 8)
        + (buf[pos++] & 0xff));
  }

  public long readUnsignedInt() {
    return ((buf[pos++] & 0xff)
            + ((buf[pos++] & 0xff) << 8)
            + ((buf[pos++] & 0xff) << 16)
            + ((long) (buf[pos++] & 0xff) << 24))
        & 0xffffffffL;
  }

  public long readLong() {
    return ((buf[pos++] & 0xffL)
        + ((buf[pos++] & 0xffL) << 8)
        + ((buf[pos++] & 0xffL) << 16)
        + ((buf[pos++] & 0xffL) << 24)
        + ((buf[pos++] & 0xffL) << 32)
        + ((buf[pos++] & 0xffL) << 40)
        + ((buf[pos++] & 0xffL) << 48)
        + ((buf[pos++] & 0xffL) << 56));
  }

  public long readLongBE() {
    return (((buf[pos++] & 0xffL) << 56)
        + ((buf[pos++] & 0xffL) << 48)
        + ((buf[pos++] & 0xffL) << 40)
        + ((buf[pos++] & 0xffL) << 32)
        + ((buf[pos++] & 0xffL) << 24)
        + ((buf[pos++] & 0xffL) << 16)
        + ((buf[pos++] & 0xffL) << 8)
        + (buf[pos++] & 0xffL));
  }

  public void readBytes(byte[] dst) {
    System.arraycopy(buf, pos, dst, 0, dst.length);
    pos += dst.length;
  }

  public byte[] readBytesNullEnd() {
    int initialPosition = pos;
    int cnt = 0;
    while (readableBytes() > 0 && (buf[pos++] != 0)) {
      cnt++;
    }
    byte[] dst = new byte[cnt];
    System.arraycopy(buf, initialPosition, dst, 0, dst.length);
    return dst;
  }

  public StandardReadableByteBuf readLengthBuffer() {
    int len = this.readIntLengthEncodedNotNull();

    StandardReadableByteBuf b = new StandardReadableByteBuf(buf, pos + len);
    b.pos = pos;
    pos += len;
    return b;
  }

  public String readString(int length) {
    pos += length;
    return new String(buf, pos - length, length, StandardCharsets.UTF_8);
  }

  public String readAscii(int length) {
    pos += length;
    return new String(buf, pos - length, length, StandardCharsets.US_ASCII);
  }

  public String readStringNullEnd() {
    int initialPosition = pos;
    int cnt = 0;
    while (readableBytes() > 0 && (buf[pos++] != 0)) {
      cnt++;
    }
    return new String(buf, initialPosition, cnt, StandardCharsets.UTF_8);
  }

  public String readStringEof() {
    int initialPosition = pos;
    pos = limit;
    return new String(buf, initialPosition, pos - initialPosition, StandardCharsets.UTF_8);
  }

  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }

  public double readDoubleBE() {
    return Double.longBitsToDouble(readLongBE());
  }
}
