// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.socket;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.util.MutableByte;
import org.mariadb.jdbc.export.MaxAllowedPacketException;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.LoggerHelper;
import org.mariadb.jdbc.util.log.Loggers;

public class Reader {

  private static final VarHandle INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final int REUSABLE_BUFFER_LENGTH = 8192;
  private static final int BUFFER_SIZE = 16384;
  private static final int MAX_PACKET_SIZE = 0xffffff;
  private static final Logger logger = Loggers.getLogger(Reader.class);
  private byte headerSequence;
  private final byte[] header = new byte[4];
  private final byte[] reusableArray = new byte[REUSABLE_BUFFER_LENGTH];
  private final InputStream inputStream;
  private final int maxQuerySizeToLog;
  private final MutableByte sequence;
  private final ReadableByteBuf readBuf = new ReadableByteBuf(null, 0);
  private String serverThreadLog = "";

  /** receive buffer: bytes read from the stream and not yet consumed are in [bufPos, bufLen) */
  private final byte[] buf = new byte[BUFFER_SIZE];

  private int bufPos;
  private int bufLen;

  /**
   * Server-independent ceiling applied to received packets once authentication completes when no
   * {@code maxAllowedPacket} is configured. Set to a quarter of the JVM max heap, clamped between
   * 16Mb and 1Gb
   */
  private static final int DEFAULT_MAX_RECEIVE_PACKET =
      (int)
          Math.max(
              16L * 1024 * 1024,
              Math.min(1024L * 1024 * 1024, Runtime.getRuntime().maxMemory() / 4));

  /** Maximum packet size accepted (1Mb before authentication completes). */
  private int maxAllowedPacket = 1024 * 1024;

  /**
   * Constructor of standard socket MySQL packet stream reader.
   *
   * @param in stream
   * @param conf connection options
   * @param sequence current increment sequence
   */
  public Reader(InputStream in, Configuration conf, MutableByte sequence) {
    this.inputStream = in;
    this.maxQuerySizeToLog = conf.maxQuerySizeToLog();
    this.sequence = sequence;
  }

  public ReadableByteBuf readableBufFromArray(byte[] buf) {
    readBuf.buf(buf, buf.length, 0);
    return readBuf;
  }

  public ReadableByteBuf readReusablePacket() throws IOException {
    return readReusablePacket(logger.isTraceEnabled());
  }

  public ReadableByteBuf readReusablePacket(boolean traceEnable) throws IOException {
    int lastPacketLength = readHeader();
    checkMaxAllowedLength(lastPacketLength);
    sequence.set(headerSequence);

    byte[] rawBytes;
    if (lastPacketLength < REUSABLE_BUFFER_LENGTH) {
      rawBytes = reusableArray;
    } else {
      rawBytes = new byte[lastPacketLength];
    }
    readFully(rawBytes, 0, lastPacketLength);

    if (traceEnable) {
      INT_LE.set(header, 0, lastPacketLength | (headerSequence << 24));
      logger.trace(
          "read: {}\n{}",
          serverThreadLog,
          LoggerHelper.hex(header, rawBytes, 0, lastPacketLength, maxQuerySizeToLog));
    }

    readBuf.buf(rawBytes, lastPacketLength, 0);
    return readBuf;
  }

  /**
   * Get the next MySQL packet. If the packet is more than 16M, read as many packets needed to
   * finish reading MySQL packet. (first that has no length = 16Mb)
   *
   * @param traceEnable must trace packet.
   * @return array packet.
   * @throws IOException if socket exception occur.
   */
  public byte[] readPacket(boolean traceEnable) throws IOException {
    int packetLength = readHeader();
    checkMaxAllowedLength(packetLength);
    byte[] rawBytes = new byte[packetLength];
    readFully(rawBytes, 0, packetLength);

    if (traceEnable) {
      INT_LE.set(header, 0, packetLength | (headerSequence << 24));
      logger.trace(
          "read: {}\n{}",
          serverThreadLog,
          LoggerHelper.hex(header, rawBytes, 0, packetLength, maxQuerySizeToLog));
    }

    if (packetLength == MAX_PACKET_SIZE) {
      return readMultiPacket(rawBytes, traceEnable);
    }
    return rawBytes;
  }

  /**
   * Reassemble a MySQL packet split over several 16Mb fragments. Fragments are buffered as they
   * arrive and copied once into the final array: growing and copying the accumulated content for
   * every fragment would copy O(n²/16Mb) bytes for an n-byte packet.
   *
   * @param firstFragment first (full 16Mb) fragment, already read
   * @param traceEnable must trace packet
   * @return complete packet content
   * @throws IOException if socket exception occur
   */
  private byte[] readMultiPacket(byte[] firstFragment, boolean traceEnable) throws IOException {
    List<byte[]> fragments = new ArrayList<>();
    fragments.add(firstFragment);
    long totalLength = firstFragment.length;

    int packetLength;
    do {
      packetLength = readHeader();
      totalLength += packetLength;
      checkMaxAllowedLength(totalLength);
      byte[] fragment = new byte[packetLength];
      readFully(fragment, 0, packetLength);

      if (traceEnable) {
        INT_LE.set(header, 0, packetLength | (headerSequence << 24));
        logger.trace(
            "read: {}\n{}",
            serverThreadLog,
            LoggerHelper.hex(header, fragment, 0, packetLength, maxQuerySizeToLog));
      }
      fragments.add(fragment);
    } while (packetLength == MAX_PACKET_SIZE);

    // checkMaxAllowedLength bounds totalLength to maxAllowedPacket, so it fits an int
    byte[] rawBytes = new byte[(int) totalLength];
    int off = 0;
    for (byte[] fragment : fragments) {
      System.arraycopy(fragment, 0, rawBytes, off, fragment.length);
      off += fragment.length;
    }
    return rawBytes;
  }

  /**
   * Read the header of the next packet, without reading its payload. The payload is then read
   * through {@link #read(byte[], int, int)}, {@link #readFully(byte[], int, int)}, {@link
   * #readByte()} and {@link #skip(long)}. A payload of {@code 0xffffff} bytes is followed by
   * another packet continuing the same payload.
   *
   * @param traceEnable log the header
   * @return payload length of the packet
   * @throws IOException if socket error occurs
   */
  public int readPacketHeader(boolean traceEnable) throws IOException {
    int packetLength = readHeader();
    checkMaxAllowedLength(packetLength);
    sequence.set(headerSequence);
    if (traceEnable) {
      INT_LE.set(header, 0, packetLength | (headerSequence << 24));
      logger.trace(
          "read: {}\n{} (payload of {} bytes read sequentially)",
          serverThreadLog,
          LoggerHelper.hex(header, new byte[0], 0, 0, maxQuerySizeToLog),
          packetLength);
    }
    return packetLength;
  }

  /**
   * Read up to {@code length} bytes of the current packet payload.
   *
   * @param dest destination array
   * @param off destination offset
   * @param length maximum number of bytes to read
   * @return number of bytes read, at least 1
   * @throws IOException if socket error occurs or the socket is closed
   */
  public int read(byte[] dest, int off, int length) throws IOException {
    int available = bufLen - bufPos;
    if (available == 0) {
      if (length >= buf.length) {
        // more than a buffer: straight from the stream, no intermediate copy
        int count = inputStream.read(dest, off, length);
        if (count < 0) {
          throw new EOFException("unexpected end of stream (socket was closed by server)");
        }
        return count;
      }
      fill();
      available = bufLen;
    }
    int count = Math.min(available, length);
    System.arraycopy(buf, bufPos, dest, off, count);
    bufPos += count;
    return count;
  }

  /**
   * Read one byte of the current packet payload.
   *
   * @return byte value in 0-255 range
   * @throws IOException if socket error occurs or the socket is closed
   */
  public int readByte() throws IOException {
    if (bufPos == bufLen) fill();
    return buf[bufPos++] & 0xff;
  }

  /**
   * Skip {@code length} bytes of the current packet payload.
   *
   * @param length number of bytes to skip
   * @throws IOException if socket error occurs or the socket is closed
   */
  public void skip(long length) throws IOException {
    long remaining = length;
    while (remaining > 0) {
      if (bufPos == bufLen) fill();
      int count = (int) Math.min(bufLen - bufPos, remaining);
      bufPos += count;
      remaining -= count;
    }
  }

  /**
   * Read exactly {@code length} bytes of the current packet payload.
   *
   * @param dest destination array
   * @param off destination offset
   * @param length number of bytes to read
   * @throws IOException if socket exception occur or the stream ends early
   */
  public void readFully(byte[] dest, int off, int length) throws IOException {
    int remaining = length;
    int available = bufLen - bufPos;
    if (available > 0) {
      int count = Math.min(available, remaining);
      System.arraycopy(buf, bufPos, dest, off, count);
      bufPos += count;
      off += count;
      remaining -= count;
    }
    while (remaining > 0) {
      if (remaining >= buf.length) {
        // the buffer is empty here: large payloads go straight into the destination
        int count = inputStream.read(dest, off, remaining);
        if (count < 0) {
          throw new EOFException(
              "unexpected end of stream, read "
                  + (length - remaining)
                  + " bytes from "
                  + length
                  + " (socket was closed by server)");
        }
        off += count;
        remaining -= count;
      } else {
        int read = inputStream.read(buf, 0, buf.length);
        if (read < 0) {
          throw new EOFException(
              "unexpected end of stream, read "
                  + (length - remaining)
                  + " bytes from "
                  + length
                  + " (socket was closed by server)");
        }
        bufLen = read;
        int count = Math.min(read, remaining);
        System.arraycopy(buf, 0, dest, off, count);
        bufPos = count;
        off += count;
        remaining -= count;
      }
    }
  }

  public int getMaxAllowedPacket() {
    return maxAllowedPacket;
  }

  public void setMaxAllowedPacket(Integer maxAllowedPacket) {
    this.maxAllowedPacket =
        (maxAllowedPacket != null) ? maxAllowedPacket : DEFAULT_MAX_RECEIVE_PACKET;
  }

  private void checkMaxAllowedLength(long length) throws MaxAllowedPacketException {
    // checked on every packet: a primitive, never null (the default applies when unset)
    if (length > maxAllowedPacket) {
      throw new MaxAllowedPacketException(
          "received packet size ("
              + length
              + ") is greater than maxAllowedPacket ("
              + maxAllowedPacket
              + ")",
          true);
    }
  }

  public MutableByte getSequence() {
    return sequence;
  }

  public void close() throws IOException {
    // nothing may be served from the buffer once closed: a streaming result set of a closed
    // connection must fail on its next row, not read what had been received ahead
    bufPos = 0;
    bufLen = 0;
    inputStream.close();
  }

  public void setServerThreadId(Long serverThreadId, HostAddress hostAddress) {
    Boolean isMaster = hostAddress != null ? hostAddress.primary : null;
    this.serverThreadLog =
        "conn="
            + (serverThreadId == null ? "-1" : serverThreadId)
            + ((isMaster != null) ? " (" + (isMaster ? "M" : "S") + ")" : "");
  }

  /**
   * Refill the (empty) receive buffer with whatever the stream has, at least one byte.
   *
   * @throws IOException if socket error occurs or the stream ended
   */
  private void fill() throws IOException {
    int count = inputStream.read(buf, 0, buf.length);
    if (count < 0) {
      throw new EOFException("unexpected end of stream (socket was closed by server)");
    }
    bufPos = 0;
    bufLen = count;
  }

  /**
   * Read the 4-byte packet header: 3-byte little-endian payload length, then the sequence byte.
   *
   * @return payload length
   * @throws IOException if socket error occurs or the stream ended
   */
  private int readHeader() throws IOException {
    if (bufLen - bufPos < 4) {
      // the header may straddle two fills: move what is left to the front and read on
      int available = bufLen - bufPos;
      System.arraycopy(buf, bufPos, buf, 0, available);
      bufPos = 0;
      bufLen = available;
      do {
        int count = inputStream.read(buf, bufLen, buf.length - bufLen);
        if (count < 0) {
          throw new EOFException(
              "unexpected end of stream, read "
                  + bufLen
                  + " bytes from 4 (socket was closed by server)");
        }
        bufLen += count;
      } while (bufLen < 4);
    }
    int h = (int) INT_LE.get(buf, bufPos);
    bufPos += 4;
    headerSequence = (byte) (h >>> 24);
    return h & 0xffffff;
  }
}
