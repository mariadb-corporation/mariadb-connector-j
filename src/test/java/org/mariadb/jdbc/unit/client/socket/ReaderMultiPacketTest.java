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
import org.mariadb.jdbc.client.socket.impl.PacketReader;
import org.mariadb.jdbc.client.util.MutableByte;

/**
 * Guards CONJ-1332: {@link PacketReader} must refuse the multipart reassembly triggered by
 * max-length (16Mb) packets until authentication has completed. A single 16Mb packet is fine, but
 * growing the buffer beyond it would let a malicious or MitM'd server stream endless max-length
 * fragments and exhaust client memory before authentication.
 */
public class ReaderMultiPacketTest {

  private static final int MAX_PACKET_SIZE = 0xffffff;

  @Test
  void readPacket_rejectsReassemblyBeforeAuthentication() throws Exception {
    // a full 16Mb fragment (which announces a continuation) followed by another fragment: the first
    // 16Mb is read, then reassembly must be refused before the buffer grows past 16Mb.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);
    writeHeader(out, 1, (byte) 1);
    out.write(new byte[] {0x42}, 0, 1);

    PacketReader reader = reader(out.toByteArray());
    IOException ex = assertThrows(IOException.class, () -> reader.readPacket(false));
    assertTrue(
        ex.getMessage().contains("authentication"), "unexpected message: " + ex.getMessage());
  }

  @Test
  void readReusablePacket_allowsSingleMaxLengthPacketBeforeAuthentication() throws Exception {
    // readReusablePacket never reassembles: a single 16Mb packet is a legitimate, bounded load and
    // must be read even during the authentication phase.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);

    PacketReader reader = reader(out.toByteArray());
    assertEquals(MAX_PACKET_SIZE, reader.readReusablePacket().readableBytes());
  }

  @Test
  void readReusablePacket_allowsNormalPacketBeforeAuthentication() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] payload = new byte[] {0x00, 0x11, 0x22};
    writeHeader(out, payload.length, (byte) 0);
    out.write(payload, 0, payload.length);

    PacketReader reader = reader(out.toByteArray());
    assertEquals(3, reader.readReusablePacket().readableBytes());
  }

  @Test
  void permitMultiPacket_allowsReassembly() throws Exception {
    // once authentication is done, a full 16Mb fragment followed by a short terminating fragment
    // must reassemble instead of being rejected.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);
    writeHeader(out, 1, (byte) 1); // terminating fragment shorter than max -> end of packet
    out.write(new byte[] {0x42}, 0, 1);

    PacketReader reader = reader(out.toByteArray());
    reader.permitMultiPacket();
    assertEquals(MAX_PACKET_SIZE + 1, reader.readPacket(false).length);
  }

  private static PacketReader reader(byte[] stream) throws Exception {
    Configuration conf = Configuration.parse("jdbc:mariadb://localhost/");
    return new PacketReader(new ByteArrayInputStream(stream), conf, new MutableByte());
  }

  private static void writeHeader(ByteArrayOutputStream out, int len, byte sequence) {
    out.write(len & 0xff);
    out.write((len >> 8) & 0xff);
    out.write((len >> 16) & 0xff);
    out.write(sequence);
  }
}
