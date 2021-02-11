package org.mariadb.jdbc.client.socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import org.mariadb.jdbc.util.MutableInt;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.LoggerHelper;
import org.mariadb.jdbc.util.log.Loggers;

public class CompressOutputStream extends OutputStream {
  private static final Logger logger = Loggers.getLogger(CompressOutputStream.class);

  private static final int MIN_COMPRESSION_SIZE = 1536; // TCP-IP single packet
  private static final float MIN_COMPRESSION_RATIO = 0.9f;

  private static final int SMALL_BUFFER_SIZE = 8192;
  private static final int MEDIUM_BUFFER_SIZE = 128 * 1024;
  private static final int LARGE_BUFFER_SIZE = 1024 * 1024;
  private static final int MAX_PACKET_LENGTH = 0x00ffffff + 7;
  private int maxPacketLength = MAX_PACKET_LENGTH;
  private final OutputStream out;
  private final MutableInt sequence;
  private byte[] buf = new byte[SMALL_BUFFER_SIZE];
  private int pos = 7;

  public CompressOutputStream(OutputStream out, MutableInt compressionSequence) {
    this.out = out;
    this.sequence = compressionSequence;
  }

  public void setMaxAllowedPacket(int maxAllowedPacket) {
    maxPacketLength = Math.min(MAX_PACKET_LENGTH, maxAllowedPacket + 7);
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
  private void growBuffer(int len) {
    int bufLength = buf.length;
    int newCapacity;
    if (bufLength == SMALL_BUFFER_SIZE) {
      if (len + pos < MEDIUM_BUFFER_SIZE) {
        newCapacity = MEDIUM_BUFFER_SIZE;
      } else if (len + pos < LARGE_BUFFER_SIZE) {
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
    } else {
      newCapacity = maxPacketLength;
    }

    byte[] newBuf = new byte[newCapacity];
    System.arraycopy(buf, 0, newBuf, 0, pos);
    buf = newBuf;
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array starting at offset <code>off</code>
   * to this output stream. The general contract for <code>write(b, off, len)</code> is that some of
   * the bytes in the array <code>b</code> are written to the output stream in order; element <code>
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
    if (pos + len >= buf.length) growBuffer(len);
    if (pos + len >= buf.length) {
      // fill buffer, then flush
      while (len > buf.length - pos) {
        int writeLen = buf.length - pos;
        System.arraycopy(b, off, buf, pos, writeLen);
        pos += writeLen;
        writeSocket(false);
        off += writeLen;
        len -= writeLen;
      }
    }
    // enough place in buffer.
    System.arraycopy(b, off, buf, pos, len);
    pos += len;
  }

  private void writeSocket(boolean end) throws IOException {
    if (pos > 7) {
      if (pos < MIN_COMPRESSION_SIZE) {
        // *******************************************************************************
        // small packet, no compression
        // *******************************************************************************

        buf[0] = (byte) (pos - 7);
        buf[1] = (byte) ((pos - 7) >>> 8);
        buf[2] = (byte) ((pos - 7) >>> 16);
        buf[3] = sequence.incrementAndGet();
        buf[4] = 0;
        buf[5] = 0;
        buf[6] = 0;

//        if (logger.isTraceEnabled()) {
//          logger.trace("send compress: \n{}", LoggerHelper.hex(buf, 0, pos, 1000));
//        }
        out.write(buf, 0, pos);

      } else {

        // *******************************************************************************
        // compressing packet
        // *******************************************************************************
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
          try (DeflaterOutputStream deflater = new DeflaterOutputStream(baos)) {
            deflater.write(buf, 7, pos - 7);
            deflater.finish();
          }

          byte[] compressedBytes = baos.toByteArray();

          int compressLen = compressedBytes.length;

          byte[] header = new byte[7];
          header[0] = (byte) compressLen;
          header[1] = (byte) (compressLen >>> 8);
          header[2] = (byte) (compressLen >>> 16);
          header[3] = sequence.incrementAndGet();
          header[4] = (byte) (pos - 7);
          header[5] = (byte) ((pos - 7) >>> 8);
          header[6] = (byte) ((pos - 7) >>> 16);

          out.write(header, 0, 7);
          out.write(compressedBytes, 0, compressLen);
//          if (logger.isTraceEnabled()) {
//            logger.trace(
//                "send compress: \n{}",
//                LoggerHelper.hex(header, compressedBytes, 0, compressLen, 1000));
//          }
        }
      }

      if (end) {
        // if buf is big, and last query doesn't use at least half of it, resize buf to default
        // value
        if (buf.length > SMALL_BUFFER_SIZE && pos * 2 < buf.length) {
          buf = new byte[SMALL_BUFFER_SIZE];
        }
      }
      pos = 7;
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
    writeSocket(true);
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
