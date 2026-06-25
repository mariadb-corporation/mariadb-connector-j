// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz.support;

import java.io.IOException;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.socket.Writer;

/** Unified mock for MariaDB protocol writer. */
public class FuzzWriter implements Writer {
  private int pos = 0;
  private byte[] buf = new byte[1024];

  @Override
  public int pos() {
    return pos;
  }

  @Override
  public byte[] buf() {
    return buf;
  }

  @Override
  public void pos(int pos) throws IOException {
    this.pos = pos;
  }

  @Override
  public void writeByte(int value) throws IOException {}

  @Override
  public void writeShort(short value) throws IOException {}

  @Override
  public void writeInt(int value) throws IOException {}

  @Override
  public void writeLong(long value) throws IOException {}

  @Override
  public void writeDouble(double value) throws IOException {}

  @Override
  public void writeFloat(float value) throws IOException {}

  @Override
  public void writeBytes(byte[] arr) throws IOException {}

  @Override
  public void writeBytesAtPos(byte[] arr, int pos) {}

  @Override
  public void writeBytes(byte[] arr, int off, int len) throws IOException {}

  @Override
  public void writeLength(long length) throws IOException {}

  @Override
  public void writeAscii(String str) throws IOException {}

  @Override
  public void writeString(String str) throws IOException {}

  @Override
  public void writeStringEscaped(String str, boolean noBackslashEscapes) throws IOException {}

  @Override
  public void writeBytesEscaped(byte[] bytes, int len, boolean noBackslashEscapes)
      throws IOException {}

  @Override
  public void writeEmptyPacket() throws IOException {}

  @Override
  public void flush() throws IOException {}

  @Override
  public void flushPipeline() throws IOException {}

  @Override
  public boolean throwMaxAllowedLength(int length) {
    return false;
  }

  @Override
  public boolean throwMaxAllowedLengthOr16M(int length) {
    return false;
  }

  @Override
  public long getCmdLength() {
    return 0;
  }

  @Override
  public void permitTrace(boolean permitTrace) {}

  @Override
  public void setServerThreadId(Long serverThreadId, HostAddress hostAddress) {}

  @Override
  public void mark() {}

  @Override
  public boolean isMarked() {
    return false;
  }

  @Override
  public boolean hasFlushed() {
    return false;
  }

  @Override
  public void flushBufferStopAtMark() throws IOException {}

  @Override
  public boolean bufIsDataAfterMark() {
    return false;
  }

  @Override
  public byte[] resetMark() {
    return new byte[0];
  }

  @Override
  public void initPacket() {}

  @Override
  public void close() throws IOException {}

  @Override
  public byte getSequence() {
    return 0;
  }
}
