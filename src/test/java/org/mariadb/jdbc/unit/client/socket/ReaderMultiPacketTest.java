// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client.socket;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.util.MutableByte;

/**
 * {@link Reader} enforces a maximum received-packet size. Until authentication completes the limit
 * is 1Mb. Once authentication is over {@link Reader#setMaxAllowedPacket(Integer)} raises (or
 * lowers) it to the configured {@code maxAllowedPacket}.
 */
public class ReaderMultiPacketTest {

  private static final int MAX_PACKET_SIZE = 0xffffff;

  @Test
  void readPacket_rejectsPacketBeyondConnectionPhaseCap() throws Exception {
    // a packet larger than the 1Mb connection-phase cap must be rejected on its header, before its
    // body is buffered.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);

    Reader reader = reader(out.toByteArray());
    IOException ex = assertThrows(IOException.class, () -> reader.readPacket(false));
    assertTrue(
        ex.getMessage().contains("maxAllowedPacket"), "unexpected message: " + ex.getMessage());
  }

  @Test
  void readReusablePacket_rejectsPacketBeyondConnectionPhaseCap() throws Exception {
    // same guard for the single-packet read path used during authentication.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);

    Reader reader = reader(out.toByteArray());
    IOException ex = assertThrows(IOException.class, () -> reader.readReusablePacket());
    assertTrue(
        ex.getMessage().contains("maxAllowedPacket"), "unexpected message: " + ex.getMessage());
  }

  @Test
  void readReusablePacket_allowsNormalPacketBeforeAuthentication() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] payload = new byte[] {0x00, 0x11, 0x22};
    writeHeader(out, payload.length, (byte) 0);
    out.write(payload, 0, payload.length);

    Reader reader = reader(out.toByteArray());
    assertEquals(3, reader.readReusablePacket().readableBytes());
  }

  @Test
  void readReusablePacket_rejectsSinglePacketBeyondMaxAllowedPacket() throws Exception {
    // a configured maxAllowedPacket smaller than the connection-phase cap must reject even a single
    // (non-reassembled) packet larger than the limit.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, 200, (byte) 0);

    Reader reader = reader(out.toByteArray());
    reader.setMaxAllowedPacket(100);
    IOException ex = assertThrows(IOException.class, () -> reader.readReusablePacket());
    assertTrue(
        ex.getMessage().contains("maxAllowedPacket"), "unexpected message: " + ex.getMessage());
  }

  @Test
  void setMaxAllowedPacket_allowsReassemblyWithinLimit() throws Exception {
    // once authentication is done and the limit is raised above the reassembled size, a full 16Mb
    // fragment followed by a short terminating fragment must reassemble instead of being rejected.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);
    writeHeader(out, 1, (byte) 1); // terminating fragment shorter than max -> end of packet
    out.write(new byte[] {0x42}, 0, 1);

    Reader reader = reader(out.toByteArray());
    reader.setMaxAllowedPacket(MAX_PACKET_SIZE + 1);
    assertEquals(MAX_PACKET_SIZE + 1, reader.readPacket(false).length);
  }

  @Test
  void readPacket_rejectsReassemblyBeyondConfiguredMaxAllowedPacket() throws Exception {
    // reassembly is permitted, but a reassembled packet larger than the configured maxAllowedPacket
    // must be rejected rather than buffered.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);
    writeHeader(out, 1, (byte) 1);
    out.write(new byte[] {0x42}, 0, 1);

    Reader reader = reader(out.toByteArray());
    reader.setMaxAllowedPacket(MAX_PACKET_SIZE);
    IOException ex = assertThrows(IOException.class, () -> reader.readPacket(false));
    assertTrue(
        ex.getMessage().contains("maxAllowedPacket"), "unexpected message: " + ex.getMessage());
  }

  @Test
  void readPacket_reassemblesContentAcrossFragmentBoundaries() throws Exception {
    // three fragments: two full 16Mb fragments and a short terminating one. Content must be
    // contiguous and in order, in particular around each fragment boundary.
    int total = 2 * MAX_PACKET_SIZE + 1000;
    byte[] payload = new byte[total];
    for (int i = 0; i < total; i++) payload[i] = (byte) (i % 251);

    ByteArrayOutputStream out = new ByteArrayOutputStream(total + 12);
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(payload, 0, MAX_PACKET_SIZE);
    writeHeader(out, MAX_PACKET_SIZE, (byte) 1);
    out.write(payload, MAX_PACKET_SIZE, MAX_PACKET_SIZE);
    writeHeader(out, 1000, (byte) 2);
    out.write(payload, 2 * MAX_PACKET_SIZE, 1000);

    Reader reader = reader(out.toByteArray());
    reader.setMaxAllowedPacket(total);
    byte[] packet = reader.readPacket(false);
    assertArrayEquals(payload, packet);
  }

  @Test
  void readPacket_acceptsEmptyTerminatingFragment() throws Exception {
    // a payload of exactly 16Mb is sent as a full fragment followed by an empty one.
    ByteArrayOutputStream out = new ByteArrayOutputStream(MAX_PACKET_SIZE + 8);
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);
    writeHeader(out, 0, (byte) 1);

    Reader reader = reader(out.toByteArray());
    reader.setMaxAllowedPacket(MAX_PACKET_SIZE + 1);
    assertEquals(MAX_PACKET_SIZE, reader.readPacket(false).length);
  }

  @Test
  void readPacket_reportsTruncatedFragment() throws Exception {
    // stream ends in the middle of the second fragment: must fail with EOF, not return partial
    // data.
    ByteArrayOutputStream out = new ByteArrayOutputStream(MAX_PACKET_SIZE + 100);
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);
    writeHeader(out, 500, (byte) 1);
    out.write(new byte[100], 0, 100);

    Reader reader = reader(out.toByteArray());
    reader.setMaxAllowedPacket(MAX_PACKET_SIZE + 500);
    IOException ex = assertThrows(IOException.class, () -> reader.readPacket(false));
    assertTrue(
        ex.getMessage().contains("unexpected end of stream"),
        "unexpected message: " + ex.getMessage());
  }

  private static Reader reader(byte[] stream) throws Exception {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/");
    return new Reader(new ByteArrayInputStream(stream), conf, new MutableByte());
  }

  private static void writeHeader(ByteArrayOutputStream out, int len, byte sequence) {
    out.write(len & 0xff);
    out.write((len >> 8) & 0xff);
    out.write((len >> 16) & 0xff);
    out.write(sequence);
  }
}
