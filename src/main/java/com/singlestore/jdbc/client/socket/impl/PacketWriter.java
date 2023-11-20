// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.socket.impl;

import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.client.socket.Writer;
import com.singlestore.jdbc.client.util.MutableByte;
import com.singlestore.jdbc.export.MaxAllowedPacketException;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.LoggerHelper;
import com.singlestore.jdbc.util.log.Loggers;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Packet writer */
@SuppressWarnings("SameReturnValue")
public class PacketWriter implements Writer {

  /** initial buffer size */
  public static final int SMALL_BUFFER_SIZE = 8192;

  private final Logger logger;
  private static final byte QUOTE = (byte) '\'';
  private static final byte DBL_QUOTE = (byte) '"';
  private static final byte ZERO_BYTE = (byte) '\0';
  private static final byte BACKSLASH = (byte) '\\';
  private static final int MEDIUM_BUFFER_SIZE = 128 * 1024;
  private static final int LARGE_BUFFER_SIZE = 1024 * 1024;
  private static final int MAX_PACKET_LENGTH = 0x00ffffff + 4;
  private final int maxQuerySizeToLog;
  private final OutputStream out;
  private int maxPacketLength = MAX_PACKET_LENGTH;
  private Integer maxAllowedPacket;
  private long cmdLength;
  private boolean permitTrace = true;
  private String serverThreadLog = "";
  private int mark = -1;
  private boolean bufContainDataAfterMark = false;

  /** internal buffer */
  protected byte[] buf;
  /** buffer position */
  protected int pos = 4;
  /** packet sequence */
  protected final MutableByte sequence;
  /** compressed packet sequence */
  protected final MutableByte compressSequence;

  /**
   * Common feature to write data into socket, creating SingleStore Packet.
   *
   * @param out output stream
   * @param maxQuerySizeToLog maximum query size to log
   * @param sequence packet sequence
   * @param maxAllowedPacket max allowed packet value if known
   * @param compressSequence compressed packet sequence
   */
  public PacketWriter(
      OutputStream out,
      int maxQuerySizeToLog,
      Integer maxAllowedPacket,
      MutableByte sequence,
      MutableByte compressSequence) {
    this.out = out;
    this.buf = new byte[SMALL_BUFFER_SIZE];
    this.maxQuerySizeToLog = maxQuerySizeToLog;
    this.cmdLength = 0;
    this.sequence = sequence;
    this.compressSequence = compressSequence;
    this.maxAllowedPacket = maxAllowedPacket;
    this.logger = Loggers.getLogger(PacketWriter.class);
  }

  /**
   * get current position
   *
   * @return current position
   */
  @Override
  public int pos() {
    return pos;
  }

  /**
   * position setter
   *
   * @param pos new position
   * @throws IOException if buffer is not big enough to contains new position
   */
  @Override
  public void pos(int pos) throws IOException {
    if (pos > buf.length) growBuffer(pos);
    this.pos = pos;
  }

  /**
   * get current command length
   *
   * @return current command length
   */
  @Override
  public long getCmdLength() {
    return cmdLength;
  }

  /**
   * Write byte into buf, flush buf to socket if needed.
   *
   * @param value byte to send
   * @throws IOException if socket error occur.
   */
  public void writeByte(int value) throws IOException {
    if (pos >= buf.length) {
      if (pos >= maxPacketLength && !bufContainDataAfterMark) {
        // buf is more than a Packet, must flushbuf()
        writeSocket(false);
      } else {
        growBuffer(1);
      }
    }
    buf[pos++] = (byte) value;
  }

  /**
   * Write short value into buf. flush buf if too small.
   *
   * @param value short value
   * @throws IOException if socket error occur
   */
  public void writeShort(short value) throws IOException {
    if (2 > buf.length - pos) {
      // not enough space remaining
      writeBytes(new byte[] {(byte) value, (byte) (value >> 8)}, 0, 2);
      return;
    }

    buf[pos] = (byte) value;
    buf[pos + 1] = (byte) (value >> 8);
    pos += 2;
  }

  /**
   * Write int value into buf. flush buf if too small.
   *
   * @param value int value
   * @throws IOException if socket error occur
   */
  public void writeInt(int value) throws IOException {
    if (4 > buf.length - pos) {
      // not enough space remaining
      byte[] arr = new byte[4];
      arr[0] = (byte) value;
      arr[1] = (byte) (value >> 8);
      arr[2] = (byte) (value >> 16);
      arr[3] = (byte) (value >> 24);
      writeBytes(arr, 0, 4);
      return;
    }

    buf[pos] = (byte) value;
    buf[pos + 1] = (byte) (value >> 8);
    buf[pos + 2] = (byte) (value >> 16);
    buf[pos + 3] = (byte) (value >> 24);
    pos += 4;
  }

  /**
   * Write long value into buf. flush buf if too small.
   *
   * @param value long value
   * @throws IOException if socket error occur
   */
  public void writeLong(long value) throws IOException {
    if (8 > buf.length - pos) {
      // not enough space remaining
      byte[] arr = new byte[8];
      arr[0] = (byte) value;
      arr[1] = (byte) (value >> 8);
      arr[2] = (byte) (value >> 16);
      arr[3] = (byte) (value >> 24);
      arr[4] = (byte) (value >> 32);
      arr[5] = (byte) (value >> 40);
      arr[6] = (byte) (value >> 48);
      arr[7] = (byte) (value >> 56);
      writeBytes(arr, 0, 8);
      return;
    }

    buf[pos] = (byte) value;
    buf[pos + 1] = (byte) (value >> 8);
    buf[pos + 2] = (byte) (value >> 16);
    buf[pos + 3] = (byte) (value >> 24);
    buf[pos + 4] = (byte) (value >> 32);
    buf[pos + 5] = (byte) (value >> 40);
    buf[pos + 6] = (byte) (value >> 48);
    buf[pos + 7] = (byte) (value >> 56);
    pos += 8;
  }

  public void writeDouble(double value) throws IOException {
    writeLong(Double.doubleToLongBits(value));
  }

  public void writeFloat(float value) throws IOException {
    writeInt(Float.floatToIntBits(value));
  }

  public void writeBytes(byte[] arr) throws IOException {
    writeBytes(arr, 0, arr.length);
  }

  public void writeBytesAtPos(byte[] arr, int pos) {
    System.arraycopy(arr, 0, buf, pos, arr.length);
  }

  /**
   * Write byte array to buf. If buf is full, flush socket.
   *
   * @param arr byte array
   * @param off offset
   * @param len byte length to write
   * @throws IOException if socket error occur
   */
  public void writeBytes(byte[] arr, int off, int len) throws IOException {
    if (len > buf.length - pos) {
      if (buf.length != maxPacketLength) {
        growBuffer(len);
      }

      // max buf size
      if (len > buf.length - pos) {

        if (mark != -1) {
          growBuffer(len);
          if (mark != -1) {
            flushBufferStopAtMark();
          }
        }

        if (len > buf.length - pos) {
          // not enough space in buf, will stream :
          // fill buf and flush until all data are snd
          int remainingLen = len;
          do {
            int lenToFillbuf = Math.min(maxPacketLength - pos, remainingLen);
            System.arraycopy(arr, off, buf, pos, lenToFillbuf);
            remainingLen -= lenToFillbuf;
            off += lenToFillbuf;
            pos += lenToFillbuf;
            if (remainingLen > 0) {
              writeSocket(false);
            } else {
              break;
            }
          } while (true);
          return;
        }
      }
    }

    System.arraycopy(arr, off, buf, pos, len);
    pos += len;
  }

  /**
   * Write field length into buf, flush socket if needed.
   *
   * @param length field length
   * @throws IOException if socket error occur.
   */
  public void writeLength(long length) throws IOException {
    if (length < 251) {
      writeByte((byte) length);
      return;
    }

    if (length < 65536) {

      if (3 > buf.length - pos) {
        // not enough space remaining
        byte[] arr = new byte[3];
        arr[0] = (byte) 0xfc;
        arr[1] = (byte) length;
        arr[2] = (byte) (length >>> 8);
        writeBytes(arr, 0, 3);
        return;
      }

      buf[pos] = (byte) 0xfc;
      buf[pos + 1] = (byte) length;
      buf[pos + 2] = (byte) (length >>> 8);
      pos += 3;
      return;
    }

    if (length < 16777216) {

      if (4 > buf.length - pos) {
        // not enough space remaining
        byte[] arr = new byte[4];
        arr[0] = (byte) 0xfd;
        arr[1] = (byte) length;
        arr[2] = (byte) (length >>> 8);
        arr[3] = (byte) (length >>> 16);
        writeBytes(arr, 0, 4);
        return;
      }

      buf[pos] = (byte) 0xfd;
      buf[pos + 1] = (byte) length;
      buf[pos + 2] = (byte) (length >>> 8);
      buf[pos + 3] = (byte) (length >>> 16);
      pos += 4;
      return;
    }

    if (9 > buf.length - pos) {
      // not enough space remaining
      byte[] arr = new byte[9];
      arr[0] = (byte) 0xfe;
      arr[1] = (byte) length;
      arr[2] = (byte) (length >>> 8);
      arr[3] = (byte) (length >>> 16);
      arr[4] = (byte) (length >>> 24);
      arr[5] = (byte) (length >>> 32);
      arr[6] = (byte) (length >>> 40);
      arr[7] = (byte) (length >>> 48);
      arr[8] = (byte) (length >>> 56);
      writeBytes(arr, 0, 9);
      return;
    }

    buf[pos] = (byte) 0xfe;
    buf[pos + 1] = (byte) length;
    buf[pos + 2] = (byte) (length >>> 8);
    buf[pos + 3] = (byte) (length >>> 16);
    buf[pos + 4] = (byte) (length >>> 24);
    buf[pos + 5] = (byte) (length >>> 32);
    buf[pos + 6] = (byte) (length >>> 40);
    buf[pos + 7] = (byte) (length >>> 48);
    buf[pos + 8] = (byte) (length >>> 56);
    pos += 9;
  }

  public void writeAscii(String str) throws IOException {
    byte[] arr = str.getBytes(StandardCharsets.US_ASCII);
    writeBytes(arr, 0, arr.length);
  }

  public void writeString(String str) throws IOException {
    int charsLength = str.length();

    // not enough space remaining
    if (charsLength * 3 >= buf.length - pos) {
      byte[] arr = str.getBytes(StandardCharsets.UTF_8);
      writeBytes(arr, 0, arr.length);
      return;
    }

    // create UTF-8 byte array
    // since java char are internally using UTF-16 using surrogate's pattern, 4 bytes unicode
    // characters will
    // represent 2 characters : example "\uD83C\uDFA4" = 🎤 unicode 8 "no microphones"
    // so max size is 3 * charLength
    // (escape characters are 1 byte encoded, so length might only be 2 when escape)
    // + 2 for the quotes for text protocol
    int charsOffset = 0;
    char currChar;

    // quick loop if only ASCII chars for faster escape
    for (;
        charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80;
        charsOffset++) {
      buf[pos++] = (byte) currChar;
    }

    // if quick loop not finished
    while (charsOffset < charsLength) {
      currChar = str.charAt(charsOffset++);
      if (currChar < 0x80) {
        buf[pos++] = (byte) currChar;
      } else if (currChar < 0x800) {
        buf[pos++] = (byte) (0xc0 | (currChar >> 6));
        buf[pos++] = (byte) (0x80 | (currChar & 0x3f));
      } else if (currChar >= 0xD800 && currChar < 0xE000) {
        // reserved for surrogate - see https://en.wikipedia.org/wiki/UTF-16
        if (currChar < 0xDC00) {
          // is high surrogate
          if (charsOffset + 1 > charsLength) {
            buf[pos++] = (byte) 0x63;
          } else {
            char nextChar = str.charAt(charsOffset);
            if (nextChar >= 0xDC00 && nextChar < 0xE000) {
              // is low surrogate
              int surrogatePairs =
                  ((currChar << 10) + nextChar) + (0x010000 - (0xD800 << 10) - 0xDC00);
              buf[pos++] = (byte) (0xf0 | ((surrogatePairs >> 18)));
              buf[pos++] = (byte) (0x80 | ((surrogatePairs >> 12) & 0x3f));
              buf[pos++] = (byte) (0x80 | ((surrogatePairs >> 6) & 0x3f));
              buf[pos++] = (byte) (0x80 | (surrogatePairs & 0x3f));
              charsOffset++;
            } else {
              // must have low surrogate
              buf[pos++] = (byte) 0x3f;
            }
          }
        } else {
          // low surrogate without high surrogate before
          buf[pos++] = (byte) 0x3f;
        }
      } else {
        buf[pos++] = (byte) (0xe0 | ((currChar >> 12)));
        buf[pos++] = (byte) (0x80 | ((currChar >> 6) & 0x3f));
        buf[pos++] = (byte) (0x80 | (currChar & 0x3f));
      }
    }
  }

  /**
   * Current buffer
   *
   * @return current buffer
   */
  @Override
  public byte[] buf() {
    return buf;
  }

  /**
   * Write string to socket.
   *
   * @param str string
   * @param noBackslashEscapes escape method
   * @throws IOException if socket error occur
   */
  @Override
  public void writeStringEscaped(String str, boolean noBackslashEscapes) throws IOException {

    int charsLength = str.length();

    // not enough space remaining
    if (charsLength * 3 >= buf.length - pos) {
      byte[] arr = str.getBytes(StandardCharsets.UTF_8);
      writeBytesEscaped(arr, arr.length, noBackslashEscapes);
      return;
    }

    // create UTF-8 byte array
    // since java char are internally using UTF-16 using surrogate's pattern, 4 bytes unicode
    // characters will
    // represent 2 characters : example "\uD83C\uDFA4" = 🎤 unicode 8 "no microphones"
    // so max size is 3 * charLength
    // (escape characters are 1 byte encoded, so length might only be 2 when escape)
    // + 2 for the quotes for text protocol
    int charsOffset = 0;
    char currChar;

    // quick loop if only ASCII chars for faster escape
    if (noBackslashEscapes) {
      for (;
          charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80;
          charsOffset++) {
        if (currChar == QUOTE) {
          buf[pos++] = QUOTE;
        }
        buf[pos++] = (byte) currChar;
      }
    } else {
      for (;
          charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80;
          charsOffset++) {
        if (currChar == BACKSLASH || currChar == QUOTE || currChar == 0 || currChar == DBL_QUOTE) {
          buf[pos++] = BACKSLASH;
        }
        buf[pos++] = (byte) currChar;
      }
    }

    // if quick loop not finished
    while (charsOffset < charsLength) {
      currChar = str.charAt(charsOffset++);
      if (currChar < 0x80) {
        if (noBackslashEscapes) {
          if (currChar == QUOTE) {
            buf[pos++] = QUOTE;
          }
        } else if (currChar == BACKSLASH
            || currChar == QUOTE
            || currChar == ZERO_BYTE
            || currChar == DBL_QUOTE) {
          buf[pos++] = BACKSLASH;
        }
        buf[pos++] = (byte) currChar;
      } else if (currChar < 0x800) {
        buf[pos++] = (byte) (0xc0 | (currChar >> 6));
        buf[pos++] = (byte) (0x80 | (currChar & 0x3f));
      } else if (currChar >= 0xD800 && currChar < 0xE000) {
        // reserved for surrogate - see https://en.wikipedia.org/wiki/UTF-16
        if (currChar < 0xDC00) {
          // is high surrogate
          if (charsOffset + 1 > charsLength) {
            buf[pos++] = (byte) 0x63;
          } else {
            char nextChar = str.charAt(charsOffset);
            if (nextChar >= 0xDC00 && nextChar < 0xE000) {
              // is low surrogate
              int surrogatePairs =
                  ((currChar << 10) + nextChar) + (0x010000 - (0xD800 << 10) - 0xDC00);
              buf[pos++] = (byte) (0xf0 | ((surrogatePairs >> 18)));
              buf[pos++] = (byte) (0x80 | ((surrogatePairs >> 12) & 0x3f));
              buf[pos++] = (byte) (0x80 | ((surrogatePairs >> 6) & 0x3f));
              buf[pos++] = (byte) (0x80 | (surrogatePairs & 0x3f));
              charsOffset++;
            } else {
              // must have low surrogate
              buf[pos++] = (byte) 0x3f;
            }
          }
        } else {
          // low surrogate without high surrogate before
          buf[pos++] = (byte) 0x3f;
        }
      } else {
        buf[pos++] = (byte) (0xe0 | ((currChar >> 12)));
        buf[pos++] = (byte) (0x80 | ((currChar >> 6) & 0x3f));
        buf[pos++] = (byte) (0x80 | (currChar & 0x3f));
      }
    }
  }

  /**
   * Write escape bytes to socket.
   *
   * @param bytes bytes
   * @param len len to write
   * @param noBackslashEscapes escape method
   * @throws IOException if socket error occur
   */
  public void writeBytesEscaped(byte[] bytes, int len, boolean noBackslashEscapes)
      throws IOException {
    if (len * 2 > buf.length - pos) {

      // makes buf bigger (up to 16M)
      if (buf.length != maxPacketLength) {
        growBuffer(len * 2);
      }

      // data may be bigger than buf.
      // must flush buf when full (and reset position to 0)
      if (len * 2 > buf.length - pos) {

        if (mark != -1) {
          growBuffer(len * 2);
          if (mark != -1) {
            flushBufferStopAtMark();
          }

        } else {

          // not enough space in buf, will fill buf
          if (noBackslashEscapes) {
            for (int i = 0; i < len; i++) {
              if (QUOTE == bytes[i]) {
                buf[pos++] = QUOTE;
                if (buf.length <= pos) {
                  writeSocket(false);
                }
              }
              buf[pos++] = bytes[i];
              if (buf.length <= pos) {
                writeSocket(false);
              }
            }
          } else {
            for (int i = 0; i < len; i++) {
              if (bytes[i] == QUOTE
                  || bytes[i] == BACKSLASH
                  || bytes[i] == DBL_QUOTE
                  || bytes[i] == ZERO_BYTE) {
                buf[pos++] = '\\';
                if (buf.length <= pos) {
                  writeSocket(false);
                }
              }
              buf[pos++] = bytes[i];
              if (buf.length <= pos) {
                writeSocket(false);
              }
            }
          }
          return;
        }
      }
    }

    // sure to have enough place filling buf directly
    if (noBackslashEscapes) {
      for (int i = 0; i < len; i++) {
        if (QUOTE == bytes[i]) {
          buf[pos++] = QUOTE;
        }
        buf[pos++] = bytes[i];
      }
    } else {
      for (int i = 0; i < len; i++) {
        if (bytes[i] == QUOTE
            || bytes[i] == BACKSLASH
            || bytes[i] == '"'
            || bytes[i] == ZERO_BYTE) {
          buf[pos++] = BACKSLASH; // add escape slash
        }
        buf[pos++] = bytes[i];
      }
    }
  }

  /**
   * buf growing use 4 size only to avoid creating/copying that are expensive operations. possible
   * size
   *
   * <ol>
   *   <li>SMALL_buf_SIZE = 8k (default)
   *   <li>MEDIUM_buf_SIZE = 128k
   *   <li>LARGE_buf_SIZE = 1M
   *   <li>getMaxPacketLength = 16M (+ 4 if using no compression)
   * </ol>
   *
   * @param len length to add
   */
  private void growBuffer(int len) throws IOException {
    int bufLength = buf.length;
    int newCapacity;
    if (bufLength == SMALL_BUFFER_SIZE) {
      if (len + pos <= MEDIUM_BUFFER_SIZE) {
        newCapacity = MEDIUM_BUFFER_SIZE;
      } else if (len + pos <= LARGE_BUFFER_SIZE) {
        newCapacity = LARGE_BUFFER_SIZE;
      } else {
        newCapacity = maxPacketLength;
      }
    } else if (bufLength == MEDIUM_BUFFER_SIZE) {
      if (len + pos < LARGE_BUFFER_SIZE) {
        newCapacity = LARGE_BUFFER_SIZE;
      } else {
        newCapacity = maxPacketLength;
      }
    } else if (bufContainDataAfterMark) {
      // want to add some information to buf without having the command Header
      // must grow buf until having all the query
      newCapacity = Math.max(len + pos, maxPacketLength);
    } else {
      newCapacity = maxPacketLength;
    }
    if (len + pos > newCapacity) {
      if (mark != -1) {
        // buf is > 16M with mark.
        // flush until mark, reset pos at beginning
        flushBufferStopAtMark();

        if (len + pos <= bufLength) {
          return;
        }

        // need to keep all data, buf can grow more than maxPacketLength
        // grow buf if needed
        if (bufLength == maxPacketLength) return;
        if (len + pos > newCapacity) {
          newCapacity = Math.min(maxPacketLength, len + pos);
        }
      }
    }

    byte[] newBuf = new byte[newCapacity];
    System.arraycopy(buf, 0, newBuf, 0, pos);
    buf = newBuf;
  }

  /**
   * Send empty packet.
   *
   * @throws IOException if socket error occur.
   */
  public void writeEmptyPacket() throws IOException {

    buf[0] = (byte) 0x00;
    buf[1] = (byte) 0x00;
    buf[2] = (byte) 0x00;
    buf[3] = this.sequence.incrementAndGet();
    out.write(buf, 0, 4);

    if (logger.isTraceEnabled()) {
      logger.trace(
          "send com : content length=0 {}\n{}", serverThreadLog, LoggerHelper.hex(buf, 0, 4));
    }
    out.flush();
    cmdLength = 0;
  }

  /**
   * Send packet to socket.
   *
   * @throws IOException if socket error occur.
   */
  public void flush() throws IOException {
    writeSocket(true);
    // if buf is big, and last query doesn't use at least half of it, resize buf to default
    // value
    if (buf.length > SMALL_BUFFER_SIZE && cmdLength * 2 < buf.length) {
      buf = new byte[SMALL_BUFFER_SIZE];
    }

    pos = 4;
    cmdLength = 0;
    mark = -1;
  }

  /**
   * Count query size. If query size is greater than max_allowed_packet and nothing has been already
   * send, throw an exception to avoid having the connection closed.
   *
   * @param length additional length to query size
   * @throws MaxAllowedPacketException if query has not to be send.
   */
  private void checkMaxAllowedLength(int length) throws MaxAllowedPacketException {
    if (maxAllowedPacket != null) {
      if (cmdLength + length >= maxAllowedPacket) {
        // launch exception only if no packet has been sent.
        throw new MaxAllowedPacketException(
            "query size ("
                + (cmdLength + length)
                + ") is >= to max_allowed_packet ("
                + maxAllowedPacket
                + ")",
            cmdLength != 0);
      }
    }
  }

  public boolean throwMaxAllowedLength(int length) {
    if (maxAllowedPacket != null) return cmdLength + length >= maxAllowedPacket;
    return false;
  }

  public void permitTrace(boolean permitTrace) {
    this.permitTrace = permitTrace;
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

  public void mark() {
    mark = pos;
  }

  public boolean isMarked() {
    return mark != -1;
  }

  public boolean hasFlushed() {
    return sequence.get() != -1;
  }

  /**
   * Flush to last mark.
   *
   * @throws IOException if flush fail.
   */
  public void flushBufferStopAtMark() throws IOException {
    final int end = pos;
    pos = mark;
    writeSocket(true);
    out.flush();
    initPacket();

    System.arraycopy(buf, mark, buf, pos, end - mark);
    pos += end - mark;
    mark = -1;
    bufContainDataAfterMark = true;
  }

  public boolean bufIsDataAfterMark() {
    return bufContainDataAfterMark;
  }

  /**
   * Reset mark flag and send bytes after mark flag.
   *
   * @return bytes after mark flag
   */
  public byte[] resetMark() {
    pos = mark;
    mark = -1;

    if (bufContainDataAfterMark) {
      byte[] data = Arrays.copyOfRange(buf, 4, pos);
      initPacket();
      bufContainDataAfterMark = false;
      return data;
    }
    return null;
  }

  public void initPacket() {
    sequence.set((byte) -1);
    compressSequence.set((byte) -1);
    pos = 4;
    cmdLength = 0;
  }

  /**
   * Flush the internal buf.
   *
   * @param commandEnd command end
   * @throws IOException id connection error occur.
   */
  protected void writeSocket(boolean commandEnd) throws IOException {
    if (pos > 4) {
      buf[0] = (byte) (pos - 4);
      buf[1] = (byte) ((pos - 4) >>> 8);
      buf[2] = (byte) ((pos - 4) >>> 16);
      buf[3] = this.sequence.incrementAndGet();
      checkMaxAllowedLength(pos - 4);
      out.write(buf, 0, pos);
      if (commandEnd) out.flush();
      cmdLength += pos - 4;

      if (logger.isTraceEnabled()) {
        if (permitTrace) {
          logger.trace(
              "send: {}\n{}", serverThreadLog, LoggerHelper.hex(buf, 0, pos, maxQuerySizeToLog));
        } else {
          logger.trace("send: content length={} {} com=<hidden>", pos - 4, serverThreadLog);
        }
      }

      // if last com fill the max size, must send an empty com to indicate command end.
      if (commandEnd && pos == maxPacketLength) {
        writeEmptyPacket();
      }

      pos = 4;
    }
  }

  public void close() throws IOException {
    out.close();
  }
}
