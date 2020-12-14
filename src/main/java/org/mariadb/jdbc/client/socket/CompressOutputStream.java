package org.mariadb.jdbc.client.socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import org.mariadb.jdbc.util.MutableInt;

public class CompressOutputStream extends OutputStream {
  private static final int MIN_COMPRESSION_SIZE = 1536; // TCP-IP single packet
  private static final float MIN_COMPRESSION_RATIO = 0.9f;
  private static final int MAX_PACKET_LENGTH = 0x00ffffff;

  private final OutputStream out;
  private final MutableInt sequence;
  private final byte[] header = new byte[7];

  public CompressOutputStream(OutputStream out, MutableInt compressionSequence) {
    this.out = out;
    this.sequence = compressionSequence;
  }

  /**
   * Writes <code>b.length</code> bytes from the specified byte array to this output stream. The
   * general contract for <code>write(b)</code> is that it should have exactly the same effect as
   * the call <code>write(b, 0, b.length)</code>.
   *
   * @param b the data.
   * @throws IOException if an I/O error occurs.
   * @see OutputStream#write(byte[], int, int)
   */
  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
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
    if (len < MIN_COMPRESSION_SIZE) {
      // *******************************************************************************
      // small packet, no compression
      // *******************************************************************************

      header[0] = (byte) len;
      header[1] = (byte) (len >>> 8);
      header[2] = (byte) (len >>> 16);
      header[3] = (byte) sequence.incrementAndGet();
      header[4] = 0;
      header[5] = 0;
      header[6] = 0;

      out.write(header, 0, 7);
      out.write(b, off, len);

    } else {
      // *******************************************************************************
      // compressing packet
      // *******************************************************************************
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        try (DeflaterOutputStream deflater = new DeflaterOutputStream(baos)) {
          deflater.write(b, off, len);
          deflater.finish();
        }

        byte[] compressedBytes = baos.toByteArray();

        int compressOffset = 0;
        int compressLen = compressedBytes.length;
        do {
          // send by bunch of 16M max
          int partLen = Math.min(MAX_PACKET_LENGTH, compressedBytes.length);

          header[0] = (byte) partLen;
          header[1] = (byte) (partLen >>> 8);
          header[2] = (byte) (partLen >>> 16);
          header[3] = (byte) sequence.incrementAndGet();
          header[4] = (byte) len;
          header[5] = (byte) (len >>> 8);
          header[6] = (byte) (len >>> 16);

          out.write(header, 0, 7);
          out.write(compressedBytes, compressOffset, partLen);

          compressOffset += partLen;
          compressLen -= partLen;

          if (partLen == MAX_PACKET_LENGTH) {
            // send empty packet
            header[0] = 0;
            header[1] = 0;
            header[2] = 0;
            header[3] = (byte) sequence.incrementAndGet();
            header[4] = 0;
            header[5] = 0;
            header[6] = 0;
            out.write(header, 0, 7);
          }

        } while (compressLen > 0);
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
    out.flush();
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
