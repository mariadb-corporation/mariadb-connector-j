// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.socket.impl;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.impl.StandardReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.util.MutableByte;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.LoggerHelper;
import org.mariadb.jdbc.util.log.Loggers;

/** Packet reader */
public class PacketReader implements Reader {

  private static final int REUSABLE_BUFFER_LENGTH = 1024;
  private static final int MAX_PACKET_SIZE = 0xffffff;
  private static final Logger logger = Loggers.getLogger(PacketReader.class);
  private final byte[] header = new byte[4];
  private final byte[] reusableArray = new byte[REUSABLE_BUFFER_LENGTH];
  private final InputStream inputStream;
  private final int maxQuerySizeToLog;
  private final MutableByte sequence;
  private final StandardReadableByteBuf readBuf = new StandardReadableByteBuf(null, 0);
  private String serverThreadLog = "";

  /**
   * Constructor of standard socket MySQL packet stream reader.
   *
   * @param in stream
   * @param conf connection options
   * @param sequence current increment sequence
   */
  public PacketReader(InputStream in, Configuration conf, MutableByte sequence) {
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
    // ***************************************************
    // Read 4 byte header
    // ***************************************************
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

    int lastPacketLength =
        (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
    sequence.set(header[3]);

    // prepare array
    byte[] rawBytes;
    if (lastPacketLength < REUSABLE_BUFFER_LENGTH) {
      rawBytes = reusableArray;
    } else {
      rawBytes = new byte[lastPacketLength];
    }

    // ***************************************************
    // Read content
    // ***************************************************
    remaining = lastPacketLength;
    off = 0;
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
   * Get next MySQL packet. If packet is more than 16M, read as many packet needed to finish reading
   * MySQL packet. (first that has not length = 16Mb)
   *
   * @param traceEnable must trace packet.
   * @return array packet.
   * @throws IOException if socket exception occur.
   */
  public byte[] readPacket(boolean traceEnable) throws IOException {
    // ***************************************************
    // Read 4 byte header
    // ***************************************************
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

    int lastPacketLength =
        (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);

    // prepare array
    byte[] rawBytes = new byte[lastPacketLength];

    // ***************************************************
    // Read content
    // ***************************************************
    remaining = lastPacketLength;
    off = 0;
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

    // ***************************************************
    // In case content length is big, content will be separate in many 16Mb packets
    // ***************************************************
    if (lastPacketLength == MAX_PACKET_SIZE) {
      int packetLength;
      do {
        remaining = 4;
        off = 0;
        do {
          int count = inputStream.read(header, off, remaining);
          if (count < 0) {
            throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
          }
          remaining -= count;
          off += count;
        } while (remaining > 0);

        packetLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);

        int currentbufLength = rawBytes.length;
        byte[] newRawBytes = new byte[currentbufLength + packetLength];
        System.arraycopy(rawBytes, 0, newRawBytes, 0, currentbufLength);
        rawBytes = newRawBytes;

        // ***************************************************
        // Read content
        // ***************************************************
        remaining = packetLength;
        off = currentbufLength;
        do {
          int count = inputStream.read(rawBytes, off, remaining);
          if (count < 0) {
            throw new EOFException(
                "unexpected end of stream, read "
                    + (packetLength - remaining)
                    + " bytes from "
                    + packetLength);
          }
          remaining -= count;
          off += count;
        } while (remaining > 0);

        if (traceEnable) {
          logger.trace(
              "read: {}\n{}",
              serverThreadLog,
              LoggerHelper.hex(
                  header, rawBytes, currentbufLength, packetLength, maxQuerySizeToLog));
        }

        lastPacketLength += packetLength;
      } while (packetLength == MAX_PACKET_SIZE);
    }

    return rawBytes;
  }

  public void skipPacket() throws IOException {
    if (logger.isTraceEnabled()) {
      readReusablePacket(logger.isTraceEnabled());
      return;
    }

    // ***************************************************
    // Read 4 byte header
    // ***************************************************
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

    int lastPacketLength =
        (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);

    remaining = lastPacketLength;
    do {
      remaining -= (int) inputStream.skip(remaining);
    } while (remaining > 0);

    // ***************************************************
    // In case content length is big, content will be separate in many 16Mb packets
    // ***************************************************
    if (lastPacketLength == MAX_PACKET_SIZE) {
      int packetLength;
      do {
        remaining = 4;
        off = 0;
        do {
          int count = inputStream.read(header, off, remaining);
          if (count < 0) {
            throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
          }
          remaining -= count;
          off += count;
        } while (remaining > 0);

        packetLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);

        remaining = packetLength;
        do {
          remaining -= (int) inputStream.skip(remaining);
        } while (remaining > 0);

        lastPacketLength += packetLength;
      } while (packetLength == MAX_PACKET_SIZE);
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
}
