// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.client.socket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.util.MutableByte;

/**
 * The reader buffers the stream itself. Whatever the stream delivers per call (one byte, a few, or
 * everything), packets must come out whole: headers straddling two fills, payloads larger than the
 * receive buffer, and the sequential payload API used by streaming results.
 */
public class ReaderBufferingTest {

  /** Delivers at most a random number of bytes per read call, like a slow socket. */
  private static final class ChunkedInputStream extends InputStream {
    private final byte[] data;
    private final Random random;
    private final int maxChunk;
    private int pos;

    ChunkedInputStream(byte[] data, int maxChunk, long seed) {
      this.data = data;
      this.maxChunk = maxChunk;
      this.random = new Random(seed);
    }

    @Override
    public int read() {
      return pos < data.length ? data[pos++] & 0xff : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (pos >= data.length) return -1;
      int n = Math.min(len, Math.min(1 + random.nextInt(maxChunk), data.length - pos));
      System.arraycopy(data, pos, b, off, n);
      pos += n;
      return n;
    }
  }

  private static byte[] packet(int seq, byte[] payload) {
    byte[] p = new byte[4 + payload.length];
    p[0] = (byte) payload.length;
    p[1] = (byte) (payload.length >> 8);
    p[2] = (byte) (payload.length >> 16);
    p[3] = (byte) seq;
    System.arraycopy(payload, 0, p, 4, payload.length);
    return p;
  }

  private static byte[] payload(int length, int seed) {
    byte[] p = new byte[length];
    new Random(seed).nextBytes(p);
    return p;
  }

  private static Reader reader(InputStream in) {
    return new Reader(in, new Configuration.Builder().build(), new MutableByte());
  }

  @Test
  public void packetsWholeWhateverTheChunking() throws IOException {
    // sizes around the receive buffer (16384) and the reusable array (8192)
    int[] sizes = {0, 1, 3, 4, 5, 300, 8191, 8192, 8193, 16380, 16384, 16388, 70000, 2, 1};
    ByteArrayOutputStream wire = new ByteArrayOutputStream();
    for (int i = 0; i < sizes.length; i++) {
      wire.write(packet(i, payload(sizes[i], i)));
    }
    byte[] data = wire.toByteArray();

    for (int maxChunk : new int[] {1, 3, 7, 100, 4096, 16384, 1 << 20}) {
      Reader reader = reader(new ChunkedInputStream(data, maxChunk, maxChunk));
      for (int i = 0; i < sizes.length; i++) {
        byte[] expected = payload(sizes[i], i);
        if (i % 2 == 0) {
          assertArrayEquals(expected, reader.readPacket(false), "chunk " + maxChunk + " #" + i);
        } else {
          ReadableByteBuf buf = reader.readReusablePacket(false);
          assertEquals(sizes[i], buf.readableBytes(), "chunk " + maxChunk + " #" + i);
          assertArrayEquals(expected, buf.readBytes(sizes[i]), "chunk " + maxChunk + " #" + i);
          assertEquals((byte) i, reader.getSequence().get(), "sequence #" + i);
        }
      }
      EOFException e = assertThrows(EOFException.class, () -> reader.readPacket(false));
      assertTrue(e.getMessage().contains("unexpected end of stream"), e.getMessage());
    }
  }

  @Test
  public void sequentialPayloadApi() throws IOException {
    byte[] big = payload(50000, 42);
    ByteArrayOutputStream wire = new ByteArrayOutputStream();
    wire.write(packet(0, big));
    wire.write(packet(1, new byte[] {7, 8, 9}));
    byte[] data = wire.toByteArray();

    for (int maxChunk : new int[] {1, 5, 1000, 16384, 1 << 20}) {
      Reader reader = reader(new ChunkedInputStream(data, maxChunk, maxChunk));
      assertEquals(50000, reader.readPacketHeader(false));
      assertEquals(big[0] & 0xff, reader.readByte());
      reader.skip(999);
      byte[] part = new byte[20000];
      reader.readFully(part, 0, 20000);
      for (int i = 0; i < 20000; i++) {
        assertEquals(big[1000 + i], part[i], "chunk " + maxChunk + " byte " + i);
      }
      int remaining = 50000 - 21000;
      int off = 21000;
      while (remaining > 0) {
        byte[] tmp = new byte[remaining];
        int n = reader.read(tmp, 0, remaining);
        assertTrue(n > 0);
        for (int i = 0; i < n; i++) {
          assertEquals(big[off + i], tmp[i], "chunk " + maxChunk + " byte " + (off + i));
        }
        off += n;
        remaining -= n;
      }
      // the next packet is intact after the sequential reads
      assertArrayEquals(new byte[] {7, 8, 9}, reader.readPacket(false));
    }
  }

  @Test
  public void truncatedStream() {
    byte[] data = packet(0, payload(100, 1));
    // header cut
    Reader r1 = reader(new ByteArrayInputStream(data, 0, 2));
    EOFException e1 = assertThrows(EOFException.class, () -> r1.readPacket(false));
    assertTrue(e1.getMessage().contains("read 2 bytes from 4"), e1.getMessage());
    // payload cut
    Reader r2 = reader(new ByteArrayInputStream(data, 0, 60));
    EOFException e2 = assertThrows(EOFException.class, () -> r2.readPacket(false));
    assertTrue(e2.getMessage().contains("read 56 bytes from 100"), e2.getMessage());
  }

  @Test
  public void nothingServedAfterClose() throws IOException {
    ByteArrayOutputStream wire = new ByteArrayOutputStream();
    wire.write(packet(0, new byte[] {1}));
    wire.write(packet(1, new byte[] {2}));
    Reader reader = reader(new ByteArrayInputStream(wire.toByteArray()));
    assertArrayEquals(new byte[] {1}, reader.readPacket(false));
    // the second packet was received ahead into the buffer: it must not be readable once closed
    reader.close();
    assertThrows(EOFException.class, () -> reader.readPacket(false));
  }
}
