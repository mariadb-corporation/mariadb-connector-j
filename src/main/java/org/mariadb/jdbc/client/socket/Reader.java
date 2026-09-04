// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.client.socket;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
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

/** Packet reader */
public class Reader {

  private static final int REUSABLE_BUFFER_LENGTH = 8192;
  private static final int MAX_PACKET_SIZE = 0xffffff;
  private static final Logger logger = Loggers.getLogger(Reader.class);
  private final byte[] header = new byte[4];
  private final byte[] reusableArray = new byte[REUSABLE_BUFFER_LENGTH];
  private final InputStream inputStream;
  private final int maxQuerySizeToLog;
  private final MutableByte sequence;
  private final ReadableByteBuf readBuf = new ReadableByteBuf(null, 0);
  private String serverThreadLog = "";

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
  private Integer maxAllowedPacket = 1024 * 1024;

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
    sequence.set(header[3]);

    byte[] rawBytes;
    if (lastPacketLength < REUSABLE_BUFFER_LENGTH) {
      rawBytes = reusableArray;
    } else {
      rawBytes = new byte[lastPacketLength];
    }

    // Read content
    int remaining = lastPacketLength;
    int off = 0;
    do {
      int count = inputStream.read(rawBytes, off, remaining);
      if (count < 0) {
        throw new EOFException(
            "unexpected end of stream, read "
                + (lastPacketLength - remaining)
                + " bytes from "
                + lastPacketLength
                + " (socket was closed by server)");
      }
      remaining -= count;
      off += count;
    } while (remaining > 0);

    if (traceEnable) {
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
    readFully(rawBytes, packetLength);

    if (traceEnable) {
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
      readFully(fragment, packetLength);

      if (traceEnable) {
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
   * Read exactly {@code length} bytes from the socket into the beginning of {@code dest}.
   *
   * @param dest destination array
   * @param length number of bytes to read
   * @throws IOException if socket exception occur or the stream ends early
   */
  private void readFully(byte[] dest, int length) throws IOException {
    int remaining = length;
    int off = 0;
    while (remaining > 0) {
      int count = inputStream.read(dest, off, remaining);
      if (count < 0) {
        throw new EOFException(
            "unexpected end of stream, read "
                + (length - remaining)
                + " bytes from "
                + length
                + " (socket was closed by server)");
      }
      remaining -= count;
      off += count;
    }
  }

  /**
   * Set the maximum size (in bytes) of a packet the reader will accept. Called once the
   * handshake/authentication phase is over to raise the limit from the 1Mb connection-phase cap to
   * the configured {@code maxAllowedPacket}.
   *
   * @param maxAllowedPacket maximum received packet size, or {@code null} to use the default
   *     heap-relative ceiling
   */
  public void setMaxAllowedPacket(Integer maxAllowedPacket) {
    this.maxAllowedPacket =
        (maxAllowedPacket != null) ? maxAllowedPacket : DEFAULT_MAX_RECEIVE_PACKET;
  }

  private void checkMaxAllowedLength(long length) throws MaxAllowedPacketException {
    if (maxAllowedPacket != null && length > maxAllowedPacket) {
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
    inputStream.close();
  }

  /**
   * Set server thread id.
   *
   * @param serverThreadId current server thread id.
   * @param hostAddress host information
   */
  public void setServerThreadId(Long serverThreadId, HostAddress hostAddress) {
    Boolean isMaster = hostAddress != null ? hostAddress.primary : null;
    this.serverThreadLog =
        "conn="
            + (serverThreadId == null ? "-1" : serverThreadId)
            + ((isMaster != null) ? " (" + (isMaster ? "M" : "S") + ")" : "");
  }

  private int readHeader() throws IOException {
    int remaining = 4;
    int off = 0;
    do {
      int count = inputStream.read(header, off, remaining);
      if (count < 0) {
        throw new EOFException(
            "unexpected end of stream, read "
                + off
                + " bytes from 4 (socket was closed by server)");
      }
      remaining -= count;
      off += count;
    } while (remaining > 0);

    return (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
  }
}
