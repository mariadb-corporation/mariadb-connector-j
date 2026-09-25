// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.result;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * UTF-8 {@link Reader} over an {@link InputStream} that reads bytes as needed, counts the
 * characters handed out and exposes the bytes it has read ahead, so that a {@link SequentialClob}
 * can switch its source to memory without losing data.
 */
final class Utf8StreamReader extends Reader {

  private static final int BUFFER_SIZE = 8192;

  private InputStream in;
  private final CharsetDecoder decoder =
      StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPLACE)
          .onUnmappableCharacter(CodingErrorAction.REPLACE);

  /** kept in read mode: bytes between position and limit are read ahead, not decoded yet */
  private final ByteBuffer bb = ByteBuffer.allocate(BUFFER_SIZE);

  private long consumedChars;
  private boolean eof;
  private boolean finished;

  Utf8StreamReader(InputStream in) {
    this.in = in;
    bb.limit(0);
  }

  /**
   * Number of characters returned so far.
   *
   * @return consumed characters
   */
  long consumedChars() {
    return consumedChars;
  }

  /**
   * Take the bytes read ahead from the source and not yet decoded.
   *
   * @return pending bytes
   */
  byte[] takePendingBytes() {
    byte[] pending = new byte[bb.remaining()];
    bb.get(pending);
    return pending;
  }

  /** Replace the byte source (bytes read ahead must have been taken first). */
  void setInput(InputStream in) {
    this.in = in;
    this.eof = false;
    this.finished = false;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if (len == 0) return 0;
    if (finished) return -1;
    CharBuffer cb = CharBuffer.wrap(cbuf, off, len);
    while (cb.hasRemaining()) {
      CoderResult cr = decoder.decode(bb, cb, eof);
      if (cr.isOverflow()) break;
      if (eof) {
        // everything decoded
        decoder.flush(cb);
        finished = true;
        break;
      }
      // underflow: need more bytes. Avoid blocking on the socket when characters are available
      if (cb.position() > off) break;
      fill();
    }
    int n = cb.position() - off;
    if (n == 0 && finished) return -1;
    consumedChars += n;
    return n;
  }

  private void fill() throws IOException {
    bb.compact();
    int n = in.read(bb.array(), bb.position(), bb.remaining());
    if (n < 0) {
      eof = true;
    } else {
      bb.position(bb.position() + n);
    }
    bb.flip();
  }

  @Override
  public boolean ready() {
    return bb.hasRemaining();
  }

  /**
   * Closing the reader does not consume it: the result-set skips remaining bytes when moving on.
   */
  @Override
  public void close() {}
}
