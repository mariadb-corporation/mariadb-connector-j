/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.io.output;

import org.mariadb.jdbc.internal.io.*;
import org.mariadb.jdbc.internal.util.exceptions.*;

import java.io.*;

@SuppressWarnings("RedundantThrows")
public interface PacketOutputStream {

  void startPacket(int seqNo);

  void writeEmptyPacket(int seqNo) throws IOException;

  void writeEmptyPacket() throws IOException;

  void write(int arr) throws IOException;

  void write(byte[] arr) throws IOException;

  void write(byte[] arr, int off, int len) throws IOException;

  void write(String str) throws IOException;

  void write(String str, boolean escape, boolean noBackslashEscapes) throws IOException;

  void write(InputStream is, boolean escape, boolean noBackslashEscapes) throws IOException;

  void write(InputStream is, long length, boolean escape, boolean noBackslashEscapes)
      throws IOException;

  void write(Reader reader, boolean escape, boolean noBackslashEscapes) throws IOException;

  void write(Reader reader, long length, boolean escape, boolean noBackslashEscapes)
      throws IOException;

  void writeBytesEscaped(byte[] bytes, int len, boolean noBackslashEscapes) throws IOException;

  void flush() throws IOException;

  void close() throws IOException;

  boolean checkRemainingSize(int len);

  boolean exceedMaxLength();

  OutputStream getOutputStream();

  void writeShort(short value) throws IOException;

  void writeInt(int value) throws IOException;

  void writeLong(long value) throws IOException;

  void writeBytes(byte value, int len) throws IOException;

  void writeFieldLength(long length) throws IOException;

  int getMaxAllowedPacket();

  void setMaxAllowedPacket(int maxAllowedPacket);

  void permitTrace(boolean permitTrace);

  void setServerThreadId(long serverThreadId, Boolean isMaster);

  void setTraceCache(LruTraceCache traceCache);

  void mark() throws MaxAllowedPacketException;

  boolean isMarked();

  void flushBufferStopAtMark() throws IOException;

  boolean bufferIsDataAfterMark();

  byte[] resetMark();

  int initialPacketPos();

  void checkMaxAllowedLength(int length) throws MaxAllowedPacketException;
}
