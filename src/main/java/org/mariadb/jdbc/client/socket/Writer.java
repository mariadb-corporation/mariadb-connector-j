// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.socket;

import java.io.IOException;
import org.mariadb.jdbc.HostAddress;

/** Packet Writer interface */
public interface Writer {
  /**
   * current buffer position
   *
   * @return current buffer position
   */
  int pos();

  /**
   * Current buffer
   *
   * @return current buffer
   */
  byte[] buf();

  /**
   * Set current buffer position
   *
   * @param pos position
   * @throws IOException if buffer cannot grow to position
   */
  void pos(int pos) throws IOException;

  /**
   * Write byte into buf, flush buf to socket if needed.
   *
   * @param value byte to send
   * @throws IOException if socket error occur.
   */
  void writeByte(int value) throws IOException;

  /**
   * Write short value into buf. flush buf if too small.
   *
   * @param value short value
   * @throws IOException if socket error occur
   */
  void writeShort(short value) throws IOException;

  /**
   * Write int value into buf. flush buf if too small.
   *
   * @param value int value
   * @throws IOException if socket error occur
   */
  void writeInt(int value) throws IOException;

  /**
   * Write long value into buf. flush buf if too small.
   *
   * @param value long value
   * @throws IOException if socket error occur
   */
  void writeLong(long value) throws IOException;

  /**
   * Write Double binary value to buffer
   *
   * @param value double value
   * @throws IOException if socket error occur
   */
  void writeDouble(double value) throws IOException;

  /**
   * Write float binary value to buffer
   *
   * @param value float value
   * @throws IOException if socket error occur
   */
  void writeFloat(float value) throws IOException;

  /**
   * Write byte array to buffer
   *
   * @param arr bytes
   * @throws IOException if socket error occur
   */
  void writeBytes(byte[] arr) throws IOException;

  /**
   * Write byte array to buffer at a specific position
   *
   * @param arr bytes
   * @param pos position
   */
  void writeBytesAtPos(byte[] arr, int pos);

  /**
   * Write byte array to buf. If buf is full, flush socket.
   *
   * @param arr byte array
   * @param off offset
   * @param len byte length to write
   * @throws IOException if socket error occur
   */
  void writeBytes(byte[] arr, int off, int len) throws IOException;

  /**
   * Write field length into buf, flush socket if needed.
   *
   * @param length field length
   * @throws IOException if socket error occur.
   */
  void writeLength(long length) throws IOException;

  /**
   * Write ascii string to buffer
   *
   * @param str string
   * @throws IOException if socket error occurs
   */
  void writeAscii(String str) throws IOException;

  /**
   * Write utf8 string to buffer
   *
   * @param str string
   * @throws IOException if socket error occurs
   */
  void writeString(String str) throws IOException;

  /**
   * Write string to socket.
   *
   * @param str string
   * @param noBackslashEscapes escape method
   * @throws IOException if socket error occur
   */
  void writeStringEscaped(String str, boolean noBackslashEscapes) throws IOException;

  /**
   * Write escape bytes to socket.
   *
   * @param bytes bytes
   * @param len len to write
   * @param noBackslashEscapes escape method
   * @throws IOException if socket error occur
   */
  void writeBytesEscaped(byte[] bytes, int len, boolean noBackslashEscapes) throws IOException;

  /**
   * Send empty packet.
   *
   * @throws IOException if socket error occur.
   */
  void writeEmptyPacket() throws IOException;

  /**
   * Send packet to socket.
   *
   * @throws IOException if socket error occur.
   */
  void flush() throws IOException;

  /**
   * must a max allowed length exception be thrown
   *
   * @param length command length
   * @return true if too big
   */
  boolean throwMaxAllowedLength(int length);

  /**
   * Get current command length
   *
   * @return command length
   */
  long getCmdLength();

  /**
   * Indicate if logging trace are permitted
   *
   * @param permitTrace permits trace to be logged
   */
  void permitTrace(boolean permitTrace);

  /**
   * Set server thread id.
   *
   * @param serverThreadId current server thread id.
   * @param hostAddress host information
   */
  void setServerThreadId(Long serverThreadId, HostAddress hostAddress);

  /** mark position */
  void mark();

  /**
   * has some position been marked
   *
   * @return is marked
   */
  boolean isMarked();

  /**
   * Current command has flushed packet to socket
   *
   * @return indicate if some packet have been flushed
   */
  boolean hasFlushed();

  /**
   * Flush to last mark.
   *
   * @throws IOException if flush fail.
   */
  void flushBufferStopAtMark() throws IOException;

  /**
   * Buffer has data after marked position
   *
   * @return indicate if there is data after marked position
   */
  boolean bufIsDataAfterMark();

  /**
   * Reset mark flag and send bytes after mark flag.
   *
   * @return bytes after mark flag
   */
  byte[] resetMark();

  /** reset sequences and position for sending a new packet */
  void initPacket();

  /**
   * Close socket stream
   *
   * @throws IOException if any error occurs
   */
  void close() throws IOException;
}
