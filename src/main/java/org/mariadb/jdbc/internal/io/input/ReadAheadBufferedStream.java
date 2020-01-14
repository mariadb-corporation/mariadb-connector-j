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

package org.mariadb.jdbc.internal.io.input;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Permit to buffer socket data, reading not only asked bytes, but available number of bytes when
 * possible.
 */
public class ReadAheadBufferedStream extends FilterInputStream {

  private static final int BUF_SIZE = 16384;
  private volatile byte[] buf;
  private int end;
  private int pos;

  public ReadAheadBufferedStream(InputStream in) {
    super(in);
    buf = new byte[BUF_SIZE];
  }

  /**
   * Reading one byte from cache of socket if needed.
   *
   * @return byte value
   * @throws IOException if socket reading error.
   */
  public synchronized int read() throws IOException {
    if (pos >= end) {
      fillBuffer(1);
      if (pos >= end) {
        return -1;
      }
    }
    return buf[pos++] & 0xff;
  }

  /**
   * Returing byte array, from cache of reading socket if needed.
   *
   * @param externalBuf buffer to fill
   * @param off offset
   * @param len length to read
   * @return number of added bytes
   * @throws IOException if exception during socket reading
   */
  public synchronized int read(byte[] externalBuf, int off, int len) throws IOException {

    if (len == 0) {
      return 0;
    }

    int totalReads = 0;
    while (true) {

      // read
      if (end - pos <= 0) {
        if (len - totalReads >= buf.length) {
          // buffer length is less than asked byte and buffer is empty
          // => filling directly into external buffer
          int reads = super.read(externalBuf, off + totalReads, len - totalReads);
          if (reads <= 0) {
            return (totalReads == 0) ? -1 : totalReads;
          }
          return totalReads + reads;

        } else {

          // filling internal buffer
          fillBuffer(len - totalReads);
          if (end <= 0) {
            return (totalReads == 0) ? -1 : totalReads;
          }
        }
      }

      // copy internal value to buffer.
      int copyLength = Math.min(len - totalReads, end - pos);
      System.arraycopy(buf, pos, externalBuf, off + totalReads, copyLength);
      pos += copyLength;
      totalReads += copyLength;

      if (totalReads >= len || super.available() <= 0) {
        return totalReads;
      }
    }
  }

  /**
   * Fill buffer with required length, or available bytes.
   *
   * @param minNeededBytes asked number of bytes
   * @throws IOException in case of failing reading stream.
   */
  private void fillBuffer(int minNeededBytes) throws IOException {
    int lengthToReallyRead = Math.min(BUF_SIZE, Math.max(super.available(), minNeededBytes));
    end = super.read(buf, 0, lengthToReallyRead);
    pos = 0;
  }

  public synchronized long skip(long n) throws IOException {
    throw new IOException("Skip from socket not implemented");
  }

  public synchronized int available() throws IOException {
    throw new IOException("available from socket not implemented");
  }

  public synchronized void reset() throws IOException {
    throw new IOException("reset from socket not implemented");
  }

  public boolean markSupported() {
    return false;
  }

  public void close() throws IOException {
    super.close();
  }
}
