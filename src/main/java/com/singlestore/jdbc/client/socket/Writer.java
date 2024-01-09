// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021-2023 SingleStore, Inc.

package com.singlestore.jdbc.client.socket;

import com.singlestore.jdbc.HostAddress;
import java.io.IOException;

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

  /** Set max allowed packet size. */
  void setMaxAllowedPacket(int maxAllowedPacket);

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

  void writeDouble(double value) throws IOException;

  void writeFloat(float value) throws IOException;

  void writeBytes(byte[] arr) throws IOException;

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

  void writeAscii(String str) throws IOException;

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
   * Send packet to buffered outputstream without flushing
   *
   * @throws IOException if socket error occur.
   */
  void flushPipeline() throws IOException;

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
