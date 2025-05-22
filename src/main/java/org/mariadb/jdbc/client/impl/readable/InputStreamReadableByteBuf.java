// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.impl.readable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;

import org.mariadb.jdbc.MariaDbBlob;
import org.mariadb.jdbc.MariaDbClob;
import org.mariadb.jdbc.StreamMariaDbBlob;
import org.mariadb.jdbc.StreamMariaDbClob;
import org.mariadb.jdbc.client.ReadableByteBuf;

/** Packet buffer that reads from an InputStream */
public final class InputStreamReadableByteBuf implements ReadableByteBuf {
  private final InputStream inputStream;
  private final byte[] buffer;
  private int pos;
  private int limit;
  private static final int BUFFER_SIZE = 8192;

  /**
   * Constructor
   *
   * @param inputStream input stream to read from
   */
  public InputStreamReadableByteBuf(InputStream inputStream) {
    this.inputStream = inputStream;
    this.buffer = new byte[BUFFER_SIZE];
    this.pos = 0;
    this.limit = 0;
  }

  public void ensureAvailable(int bytes) throws IOException {
    while (limit - pos < bytes) {
      //TODO ensure buffer has enough place
      int read =
          inputStream.read(buffer, limit, Math.min(bytes - (limit - pos), buffer.length - limit));
      if (read < 0) {
        throw new IOException("End of stream reached");
      }
      limit += read;
    }
  }

  public int readableBytes() {
    return -1;
  }

  public int pos() {
    return this.pos;
  }

  public void pos(int pos) {
    this.pos = pos;
  }

  public void skip() throws IOException {
    ensureAvailable(1);
    pos++;
  }

  public void skip(int length) throws IOException {
    ensureAvailable(length);
    pos += length;
  }

  public void skipLengthEncoded() throws IOException {
    ensureAvailable(1);
    byte len = buffer[pos++];
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

  public Blob readBlob(int length) {
    if (length <= limit - pos) {
      byte[] buffedData = new byte[limit - pos];
      System.arraycopy(buffer, pos, buffedData, 0, length);
      pos += length;
      return MariaDbBlob.safeMariaDbBlob(buffedData, 0, length);
    }

    byte[] buffedData = new byte[limit - pos];
    System.arraycopy(buffer, pos, buffedData, 0, limit - pos);
    pos = limit;
    return new StreamMariaDbBlob(
        buffedData, buffedData.length, new LimitedInputStream(inputStream, length - (limit - pos)), length - (limit - pos));
  }

  public Clob readClob(int length) {
    if (length <= limit - pos) {
      byte[] buffedData = new byte[limit - pos];
      System.arraycopy(buffer, pos, buffedData, 0, length);
      pos += length;
      return new MariaDbClob(buffedData, 0, length);
    }

    byte[] buffedData = new byte[limit - pos];
    System.arraycopy(buffer, pos, buffedData, 0, limit - pos);
    pos = limit;
    return new StreamMariaDbClob(
            buffedData, buffedData.length, new LimitedInputStream(inputStream, length - (limit - pos)), length - (limit - pos));

  }

  public InputStream readInputStream(int length) {
    InputStream is = new SequenceInputStream(
            new ByteArrayInputStream(buffer, pos, limit), new LimitedInputStream(inputStream, length - (limit - pos)));
    pos = limit;
    return is;
  }

  public byte getByte() throws IOException {
    ensureAvailable(1);
    return buffer[pos];
  }

  public byte getByte(int index) throws IOException {
    ensureAvailable(index + 1);
    return buffer[index];
  }

  public short getUnsignedByte() throws IOException {
    ensureAvailable(1);
    return (short) (buffer[pos] & 0xff);
  }

  public long readLongLengthEncodedNotNull() throws IOException {
    ensureAvailable(1);
    int type = (buffer[pos++] & 0xff);
    if (type < 251) return type;
    switch (type) {
      case 252:
        return readUnsignedShort();
      case 253:
        return readUnsignedMedium();
      default:
        return readLong();
    }
  }

  public int readIntLengthEncodedNotNull() throws IOException {
    ensureAvailable(1);
    int type = (buffer[pos++] & 0xff);
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

  public int skipIdentifier() throws IOException {
    int len = readIntLengthEncodedNotNull();
    pos += len;
    return pos;
  }

  public long atoll(int length) throws IOException {
    ensureAvailable(length);
    boolean negate = false;
    int idx = 0;
    long result = 0;

    if (length > 0 && buffer[pos] == 45) {
      negate = true;
      pos++;
      idx++;
    }

    while (idx++ < length) {
      result = result * 10 + buffer[pos++] - 48;
    }

    return (negate) ? -1 * result : result;
  }

  public long atoull(int length) throws IOException {
    ensureAvailable(length);
    long result = 0;
    for (int idx = 0; idx < length; idx++) {
      result = result * 10 + buffer[pos++] - 48;
    }
    return result;
  }

  public Integer readLength() throws IOException {
    ensureAvailable(1);
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

  public byte readByte() throws IOException {
    ensureAvailable(1);
    return buffer[pos++];
  }

  public short readUnsignedByte() throws IOException {
    ensureAvailable(1);
    return (short) (buffer[pos++] & 0xff);
  }

  public short readShort() throws IOException {
    ensureAvailable(2);
    return (short) ((buffer[pos++] & 0xff) + (buffer[pos++] << 8));
  }

  public int readUnsignedShort() throws IOException {
    ensureAvailable(2);
    return ((buffer[pos++] & 0xff) + (buffer[pos++] << 8)) & 0xffff;
  }

  public int readMedium() throws IOException {
    int value = readUnsignedMedium();
    if ((value & 0x800000) != 0) {
      value |= 0xff000000;
    }
    return value;
  }

  public int readUnsignedMedium() throws IOException {
    ensureAvailable(3);
    return ((buffer[pos++] & 0xff)
        + ((buffer[pos++] & 0xff) << 8)
        + ((buffer[pos++] & 0xff) << 16));
  }

  public int readInt() throws IOException {
    ensureAvailable(4);
    return ((buffer[pos++] & 0xff)
        + ((buffer[pos++] & 0xff) << 8)
        + ((buffer[pos++] & 0xff) << 16)
        + ((buffer[pos++] & 0xff) << 24));
  }

  public int readIntBE() throws IOException {
    ensureAvailable(4);
    return (((buffer[pos++] & 0xff) << 24)
        + ((buffer[pos++] & 0xff) << 16)
        + ((buffer[pos++] & 0xff) << 8)
        + (buffer[pos++] & 0xff));
  }

  public long readUnsignedInt() throws IOException {
    ensureAvailable(4);
    return ((buffer[pos++] & 0xff)
            + ((buffer[pos++] & 0xff) << 8)
            + ((buffer[pos++] & 0xff) << 16)
            + ((long) (buffer[pos++] & 0xff) << 24))
        & 0xffffffffL;
  }

  public long readLong() throws IOException {
    ensureAvailable(8);
    return ((buffer[pos++] & 0xffL)
        + ((buffer[pos++] & 0xffL) << 8)
        + ((buffer[pos++] & 0xffL) << 16)
        + ((buffer[pos++] & 0xffL) << 24)
        + ((buffer[pos++] & 0xffL) << 32)
        + ((buffer[pos++] & 0xffL) << 40)
        + ((buffer[pos++] & 0xffL) << 48)
        + ((buffer[pos++] & 0xffL) << 56));
  }

  public long readLongBE() throws IOException {
    ensureAvailable(8);
    return (((buffer[pos++] & 0xffL) << 56)
        + ((buffer[pos++] & 0xffL) << 48)
        + ((buffer[pos++] & 0xffL) << 40)
        + ((buffer[pos++] & 0xffL) << 32)
        + ((buffer[pos++] & 0xffL) << 24)
        + ((buffer[pos++] & 0xffL) << 16)
        + ((buffer[pos++] & 0xffL) << 8)
        + (buffer[pos++] & 0xffL));
  }

  public void readBytes(byte[] dst) throws IOException {
    ensureAvailable(dst.length);
    System.arraycopy(buffer, pos, dst, 0, dst.length);
    pos += dst.length;
  }

  public byte[] readBytesNullEnd() throws IOException {
    int initialPosition = pos;
    int cnt = 0;
    while (true) {
      ensureAvailable(1);
      if (buffer[pos] == 0) {
        pos++;
        break;
      }
      pos++;
      cnt++;
    }
    byte[] dst = new byte[cnt];
    System.arraycopy(buffer, initialPosition, dst, 0, dst.length);
    return dst;
  }

  public ReadableByteBuf readLengthBuffer() throws IOException {
    int len = this.readIntLengthEncodedNotNull();
    byte[] data = new byte[len];
    ensureAvailable(len);
    System.arraycopy(buffer, pos, data, 0, len);
    pos += len;
    return new BufferedReadableByteBuf(data);
  }

  public String readString(int length) throws IOException {
    ensureAvailable(length);
    String result = new String(buffer, pos, length, StandardCharsets.UTF_8);
    pos += length;
    return result;
  }

  public String readAscii(int length) throws IOException {
    ensureAvailable(length);
    String result = new String(buffer, pos, length, StandardCharsets.US_ASCII);
    pos += length;
    return result;
  }

  public String readStringNullEnd() throws IOException {
    int initialPosition = pos;
    int cnt = 0;
    while (true) {
      ensureAvailable(1);
      if (buffer[pos] == 0) {
        pos++;
        break;
      }
      pos++;
      cnt++;
    }
    return new String(buffer, initialPosition, cnt, StandardCharsets.UTF_8);
  }

  public String readStringEof() throws IOException {
    int initialPosition = pos;
    while (true) {
      try {
        ensureAvailable(1);
        pos++;
      } catch (IOException e) {
        break;
      }
    }
    return new String(buffer, initialPosition, pos - initialPosition, StandardCharsets.UTF_8);
  }

  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  public double readDoubleBE() throws IOException {
    return Double.longBitsToDouble(readLongBE());
  }

  private class LimitedInputStream extends FilterInputStream {
    private final long maxBytes;
    private long bytesRead;

    /**
     * Creates a new LimitedInputStream that will only read up to maxBytes from the underlying stream.
     *
     * @param in The underlying input stream
     * @param maxBytes Maximum number of bytes to read
     */
    public LimitedInputStream(InputStream in, long maxBytes) {
      super(in);
      this.maxBytes = maxBytes;
      this.bytesRead = 0;
    }

    @Override
    public int read() throws IOException {
      if (bytesRead >= maxBytes) {
        return -1; // End of stream
      }

      int result = super.read();
      if (result != -1) {
        bytesRead++;
      }
      return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (bytesRead >= maxBytes) {
        return -1; // End of stream
      }

      // Limit the length to our remaining bytes
      long remaining = maxBytes - bytesRead;
      int limitedLen = (int) Math.min(len, remaining);

      int result = super.read(b, off, limitedLen);
      if (result > 0) {
        bytesRead += result;
      }
      return result;
    }

    @Override
    public long skip(long n) throws IOException {
      long remaining = maxBytes - bytesRead;
      long toSkip = Math.min(n, remaining);

      long skipped = super.skip(toSkip);
      bytesRead += skipped;
      return skipped;
    }

    @Override
    public int available() throws IOException {
      return (int) Math.min(super.available(), maxBytes - bytesRead);
    }
  }
}
