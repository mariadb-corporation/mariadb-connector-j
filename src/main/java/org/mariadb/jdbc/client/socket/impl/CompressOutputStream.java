// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.client.socket.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import org.mariadb.jdbc.client.util.MutableInt;

public class CompressOutputStream extends OutputStream {
  private static final int MIN_COMPRESSION_SIZE = 1536; // TCP-IP single packet
  private final OutputStream out;
  private final MutableInt sequence;
  private final byte[] header = new byte[7];
  private byte[] longPacketBuffer = null;

  public CompressOutputStream(OutputStream out, MutableInt compressionSequence) {
    this.out = out;
    this.sequence = compressionSequence;
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array starting at offset <code>off</code>
   * to this output stream. The general contract for <code>write(b, off, len)</code> is that some
   * bytes in the array <code>b</code> are written to the output stream in order; element <code>
   * b[off]</code> is the first byte written and <code>b[off+len-1]</code> is the last byte written
   * by this operation.
   *
   * <p>The <code>write</code> method of <code>OutputStream</code> calls the write method of one
   * argument on each of the bytes to be written out. Subclasses are encouraged to override this
   * method and provide a more efficient implementation.
   *
   * <p>If <code>b</code> is <code>null</code>, a <code>NullPointerException</code> is thrown.
   *
   * <p>If <code>off</code> is negative, or <code>len</code> is negative, or <code>off+len</code> is
   * greater than the length of the array <code>b</code>, then an IndexOutOfBoundsException is
   * thrown.
   *
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @throws IOException if an I/O error occurs. In particular, an <code>IOException</code> is
   *     thrown if the output stream is closed.
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len + ((longPacketBuffer != null) ? longPacketBuffer.length : 0) < MIN_COMPRESSION_SIZE) {
      // *******************************************************************************
      // small packet, no compression
      // *******************************************************************************

      if (longPacketBuffer != null) {
        header[0] = (byte) (len + longPacketBuffer.length);
        header[1] = (byte) ((len + longPacketBuffer.length) >>> 8);
        header[2] = 0;
        header[3] = sequence.incrementAndGet();
        header[4] = 0;
        header[5] = 0;
        header[6] = 0;
        out.write(header, 0, 7);
        out.write(longPacketBuffer, 0, longPacketBuffer.length);
        out.write(b, off, len);
        longPacketBuffer = null;
        return;
      }

      header[0] = (byte) len;
      header[1] = (byte) (len >>> 8);
      header[2] = 0;
      header[3] = sequence.incrementAndGet();
      header[4] = 0;
      header[5] = 0;
      header[6] = 0;
      out.write(header, 0, 7);
      out.write(b, off, len);

    } else {
      // *******************************************************************************
      // compressing packet
      // *******************************************************************************
      int sent = 0;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        try (DeflaterOutputStream deflater = new DeflaterOutputStream(baos)) {

          /**
           * For multi packet, len will be 0x00ffffff + 4 bytes for header. but compression can only
           * compress up to 0x00ffffff bytes (header initial length size cannot be > 3 bytes) so,
           * for this specific case, a buffer will save remaining data
           */
          if (longPacketBuffer != null) {
            deflater.write(longPacketBuffer, 0, longPacketBuffer.length);
            sent = longPacketBuffer.length;
            longPacketBuffer = null;
          }
          if (len + sent > 0x00ffffff) {
            int remaining = len + sent - 0x00ffffff;
            longPacketBuffer = new byte[remaining];
            System.arraycopy(b, off + 0x00ffffff - sent, longPacketBuffer, 0, remaining);
          }

          int bufLenSent = Math.min(0x00ffffff - sent, len);
          deflater.write(b, off, bufLenSent);
          sent += bufLenSent;
          deflater.finish();
        }

        byte[] compressedBytes = baos.toByteArray();

        int compressLen = compressedBytes.length;

        header[0] = (byte) compressLen;
        header[1] = (byte) (compressLen >>> 8);
        header[2] = (byte) (compressLen >>> 16);
        header[3] = sequence.incrementAndGet();
        header[4] = (byte) sent;
        header[5] = (byte) (sent >>> 8);
        header[6] = (byte) (sent >>> 16);

        out.write(header, 0, 7);
        out.write(compressedBytes, 0, compressLen);
      }
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes to be written out. The general
   * contract of <code>flush</code> is that calling it is an indication that, if any bytes
   * previously written have been buffered by the implementation of the output stream, such bytes
   * should immediately be written to their intended destination.
   *
   * <p>If the intended destination of this stream is an abstraction provided by the underlying
   * operating system, for example a file, then flushing the stream guarantees only that bytes
   * previously written to the stream are passed to the operating system for writing; it does not
   * guarantee that they are actually written to a physical device such as a disk drive.
   *
   * <p>The <code>flush</code> method of <code>OutputStream</code> does nothing.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public void flush() throws IOException {
    if (longPacketBuffer != null) {
      byte[] b = longPacketBuffer;
      longPacketBuffer = null;
      write(b, 0, b.length);
    }
    out.flush();
    sequence.set((byte) -1);
  }

  /**
   * Closes this output stream and releases any system resources associated with this stream. The
   * general contract of <code>close</code> is that it closes the output stream. A closed stream
   * cannot perform output operations and cannot be reopened.
   *
   * <p>The <code>close</code> method of <code>OutputStream</code> does nothing.
   *
   * @throws IOException if an I/O error occurs.
   */
  @Override
  public void close() throws IOException {
    out.close();
  }

  /**
   * Writes the specified byte to this output stream. The general contract for <code>write</code> is
   * that one byte is written to the output stream. The byte to be written is the eight low-order
   * bits of the argument <code>b</code>. The 24 high-order bits of <code>b</code> are ignored.
   *
   * <p>Subclasses of <code>OutputStream</code> must provide an implementation for this method.
   *
   * @param b the <code>byte</code>.
   * @throws IOException if an I/O error occurs. In particular, an <code>IOException</code> may be
   *     thrown if the output stream has been closed.
   */
  @Override
  public void write(int b) throws IOException {
    throw new IOException("NOT EXPECTED !");
  }
}
