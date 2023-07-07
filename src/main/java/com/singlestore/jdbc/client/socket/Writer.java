// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2023 SingleStore, Inc.

package com.singlestore.jdbc.client.socket;

import com.singlestore.jdbc.HostAddress;
import java.io.IOException;

/** Packet Writer interface */
public interface Writer {

  int pos();

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

  boolean throwMaxAllowedLength(int length);

  long getCmdLength();

  void setMaxAllowedPacket(int maxAllowedPacket);

  void permitTrace(boolean permitTrace);

  /**
   * Set server thread id.
   *
   * @param serverThreadId current server thread id.
   * @param hostAddress host information
   */
  void setServerThreadId(Long serverThreadId, HostAddress hostAddress);

  void mark();

  boolean isMarked();

  boolean hasFlushed();

  /**
   * Flush to last mark.
   *
   * @throws IOException if flush fail.
   */
  void flushBufferStopAtMark() throws IOException;

  boolean bufIsDataAfterMark();

  /**
   * Reset mark flag and send bytes after mark flag.
   *
   * @return bytes after mark flag
   */
  byte[] resetMark();

  void initPacket();

  void close() throws IOException;
}
