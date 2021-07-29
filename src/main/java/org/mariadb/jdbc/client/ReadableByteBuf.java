// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client;

import java.nio.charset.StandardCharsets;
import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.util.MutableInt;

public final class ReadableByteBuf {
  private final MutableInt sequence;
  private int limit;
  private byte[] buf;
  private int pos;
  private int mark;

  public ReadableByteBuf(MutableInt sequence, byte[] buf, int limit) {
    this.sequence = sequence;
    this.pos = 0;
    this.buf = buf;
    this.limit = limit;
    this.mark = -1;
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

  public ReadableByteBuf buf(byte[] buf, int limit) {
    this.buf = buf;
    this.limit = limit;
    return this;
  }

  public void pos(int pos) {
    this.pos = pos;
  }

  public void mark() {
    mark = pos;
  }

  public void reset() {
    if (mark == -1) throw new IllegalStateException("mark was not set");
    pos = mark;
  }

  public void skip() {
    pos++;
  }

  public ReadableByteBuf skip(int length) {
    pos += length;
    return this;
  }

  public MariaDbBlob readBlob(int length) {
    pos += length;
    return MariaDbBlob.safeMariaDbBlob(buf, pos - length, length);
  }

  public MutableInt getSequence() {
    return sequence;
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

  public int readLengthNotNull() {
    int type = (buf[pos++] & 0xff);
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
    int type = (buf[pos++] & 0xff);
    if (type == 252) {
      pos += readUnsignedShort();
      return pos;
    }
    pos += type;
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
    return (short) ((buf[pos++] & 0xff) | (buf[pos++] << 8));
  }

  public int readUnsignedShort() {
    return ((buf[pos++] & 0xff) | (buf[pos++] << 8)) & 0xffff;
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

  public ReadableByteBuf readBytes(byte[] dst) {
    System.arraycopy(buf, pos, dst, 0, dst.length);
    pos += dst.length;
    return this;
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

  public ReadableByteBuf readLengthBuffer() {
    int len = readLengthNotNull();
    byte[] tmp = new byte[len];
    readBytes(tmp);
    return new ReadableByteBuf(sequence, tmp, len);
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
