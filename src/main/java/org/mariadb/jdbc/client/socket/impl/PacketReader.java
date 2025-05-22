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
import org.mariadb.jdbc.client.impl.readable.InputStreamReadableByteBuf;
import org.mariadb.jdbc.client.impl.readable.BufferedReadableByteBuf;
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
  private String serverThreadLog = "";

  private class LargePacketInputStream extends InputStream {
    private int packetLength;
    private byte[] buf;
    private int offset;

    public LargePacketInputStream(byte[] initialPacket) {
      this.packetLength = initialPacket.length;
      this.buf = initialPacket;
      this.offset = 0;
    }

    @Override
    public int read() throws IOException {
      if (offset >= buf.length) {
        if (!readNextPacket()) {
          return -1;
        }
      }

      return buf[offset++] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {

      int totalRead = 0;
      while (len > 0) {
        if (offset >= buf.length) {
          if (!readNextPacket()) {
            break;
          }
        }

        int toRead = Math.min(len, buf.length - offset);
        System.arraycopy(buf, offset, b, off, toRead);
        offset += toRead;
        off += toRead;
        len -= toRead;
        totalRead += toRead;
      }

      return totalRead > 0 ? totalRead : -1;
    }

    private boolean readNextPacket() throws IOException {
      if (packetLength != MAX_PACKET_SIZE) {
        return false;
      }

      // Read header
      int remaining = 4;
      int off = 0;
      do {
        int count = inputStream.read(header, off, remaining);
        if (count < 0) {
          throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
        }
        remaining -= count;
        off += count;
      } while (remaining > 0);

      packetLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
      if (packetLength == 0) {
        return false;
      }

      buf = new byte[packetLength];
      offset = 0;

      // Read content
      remaining = packetLength;
      off = 0;
      do {
        int count = inputStream.read(buf, off, remaining);
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

      if (logger.isTraceEnabled()) {
        logger.trace(
            "read: {}\n{}",
            serverThreadLog,
            LoggerHelper.hex(header, buf, 0, packetLength, maxQuerySizeToLog));
      }

      return true;
    }
  }

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

  public ReadableByteBuf readReusablePacket(boolean traceEnable) throws IOException {
    // Read header
    int remaining = 4;
    int off = 0;
    do {
      int count = inputStream.read(header, off, remaining);
      if (count < 0) {
        throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
      }
      remaining -= count;
      off += count;
    } while (remaining > 0);

    int packetLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
    sequence.set(header[3]);

    if (packetLength == 0) {
      return new BufferedReadableByteBuf(new byte[0]);
    }

    if (packetLength > MAX_PACKET_SIZE) {
      throw new IOException(
          "Packet too large: " + packetLength + " bytes (max: " + MAX_PACKET_SIZE + ")");
    }

    // Read content
    byte[] buf = packetLength <= REUSABLE_BUFFER_LENGTH ? reusableArray : new byte[packetLength];
    remaining = packetLength;
    off = 0;
    do {
      int count = inputStream.read(buf, off, remaining);
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

    if (traceEnable && logger.isTraceEnabled()) {
      logger.trace(
          "read: {}\n{}",
          serverThreadLog,
          LoggerHelper.hex(header, buf, 0, packetLength, maxQuerySizeToLog));
    }

    return new BufferedReadableByteBuf(buf, packetLength);
  }

  public ReadableByteBuf readReusablePacket() throws IOException {
    return readReusablePacket(false);
  }

  public ReadableByteBuf readPacket(boolean traceEnable, boolean permitSequentialAccess)
      throws IOException {
    // Read header
    int remaining = 4;
    int off = 0;
    do {
      int count = inputStream.read(header, off, remaining);
      if (count < 0) {
        throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
      }
      remaining -= count;
      off += count;
    } while (remaining > 0);

    int lastPacketLength =
        (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
    sequence.set(header[3]);

    if (lastPacketLength == 0) {
      return new BufferedReadableByteBuf(new byte[0]);
    }

    byte[] rawBytes = new byte[lastPacketLength];
    remaining = lastPacketLength;
    off = 0;
    do {
      int count = inputStream.read(rawBytes, off, remaining);
      if (count < 0) {
        throw new EOFException(
            "unexpected end of stream, read "
                + (lastPacketLength - remaining)
                + " bytes from "
                + lastPacketLength);
      }
      remaining -= count;
      off += count;
    } while (remaining > 0);

    if (traceEnable && logger.isTraceEnabled()) {
      logger.trace(
          "read: {}\n{}",
          serverThreadLog,
          LoggerHelper.hex(header, rawBytes, 0, lastPacketLength, maxQuerySizeToLog));
    }

    // ***************************************************
    // In case content length is big, content will be separate in many 16Mb packets
    // ***************************************************
    if (lastPacketLength == MAX_PACKET_SIZE) {
      if (permitSequentialAccess) {
        return new InputStreamReadableByteBuf(new LargePacketInputStream(rawBytes));
      } else {
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

          packetLength =
              (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);

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
    }
    //    // TODO only for testing
    //    if (permitSequentialAccess) {
    //      return new InputStreamReadableByteBuf(new LargePacketInputStream(rawBytes));
    //    }
    return new BufferedReadableByteBuf(rawBytes, lastPacketLength);
  }

  public void skipPacket() throws IOException {
    // Read header
    int remaining = 4;
    int off = 0;
    do {
      int count = inputStream.read(header, off, remaining);
      if (count < 0) {
        throw new EOFException("unexpected end of stream, read " + off + " bytes from 4");
      }
      remaining -= count;
      off += count;
    } while (remaining > 0);

    int packetLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8) + ((header[2] & 0xff) << 16);
    sequence.set(header[3]);

    if (packetLength == 0) {
      return;
    }

    if (packetLength > MAX_PACKET_SIZE) {
      throw new IOException(
          "Packet too large: " + packetLength + " bytes (max: " + MAX_PACKET_SIZE + ")");
    }

    // Skip content
    remaining = packetLength;
    while (remaining > 0) {
      long skipped = inputStream.skip(remaining);
      if (skipped <= 0) {
        throw new EOFException(
            "unexpected end of stream, skipped "
                + (packetLength - remaining)
                + " bytes from "
                + packetLength);
      }
      remaining -= (int) skipped;
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
