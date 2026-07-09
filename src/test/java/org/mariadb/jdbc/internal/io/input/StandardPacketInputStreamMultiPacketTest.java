package org.mariadb.jdbc.internal.io.input;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;
import org.mariadb.jdbc.util.Options;

/**
 * Guards CONJ-1332: {@link StandardPacketInputStream} must refuse the multipart reassembly
 * triggered by max-length (16Mb) packets until authentication has completed. The first 16Mb
 * fragment is still read, but growing the buffer beyond it would let a malicious or MitM'd server
 * stream endless max-length fragments and exhaust client memory before authentication.
 */
public class StandardPacketInputStreamMultiPacketTest {

  private static final int MAX_PACKET_SIZE = 0xffffff;

  @Test
  public void readPacket_rejectsReassemblyBeforeAuthentication() throws Exception {
    // a full 16Mb fragment announces a continuation: the first 16Mb is read, then reassembly must
    // be refused before the buffer grows past 16Mb.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);

    StandardPacketInputStream reader = reader(out.toByteArray());
    IOException ex = assertThrows(IOException.class, () -> reader.getPacketArray(false));
    assertTrue(
        "unexpected message: " + ex.getMessage(), ex.getMessage().contains("authentication"));
  }

  @Test
  public void readPacket_allowsNormalPacketBeforeAuthentication() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] payload = new byte[] {0x00, 0x11, 0x22};
    writeHeader(out, payload.length, (byte) 0);
    out.write(payload, 0, payload.length);

    StandardPacketInputStream reader = reader(out.toByteArray());
    assertEquals(payload.length, reader.getPacketArray(false).length);
  }

  @Test
  public void permitMultiPacket_allowsReassembly() throws Exception {
    // once authentication is done, a full 16Mb fragment followed by a short terminating fragment
    // must reassemble instead of being rejected.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeHeader(out, MAX_PACKET_SIZE, (byte) 0);
    out.write(new byte[MAX_PACKET_SIZE], 0, MAX_PACKET_SIZE);
    writeHeader(out, 1, (byte) 1); // terminating fragment shorter than max -> end of packet
    out.write(new byte[] {0x42}, 0, 1);

    StandardPacketInputStream reader = reader(out.toByteArray());
    reader.permitMultiPacket();
    assertEquals(MAX_PACKET_SIZE + 1, reader.getPacketArray(false).length);
  }

  private static StandardPacketInputStream reader(byte[] stream) {
    Options options = new Options();
    options.useReadAheadInput = false;
    return new StandardPacketInputStream(new ByteArrayInputStream(stream), options, 1L);
  }

  private static void writeHeader(ByteArrayOutputStream out, int len, byte sequence) {
    out.write(len & 0xff);
    out.write((len >> 8) & 0xff);
    out.write((len >> 16) & 0xff);
    out.write(sequence);
  }
}
