// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * A truncated packet from a rogue server must end in an SQLException with the socket closed, never
 * in an unchecked exception escaping the JDBC call: packet content is server-controlled, so every
 * read has to stay within the bytes the packet really holds.
 */
public class RogueServerMalformedPacketTest {

  private static final int CLIENT_PROTOCOL_41 = 512;
  private static final int SECURE_CONNECTION = 32768;
  private static final int PLUGIN_AUTH = 1 << 19;

  @FunctionalInterface
  private interface Scenario {
    void play(InputStream in, OutputStream out) throws IOException;
  }

  /** a one-byte error packet as first packet */
  @Test
  public void truncatedErrorPacketBeforeHandshake() throws Exception {
    expectSqlException(
        (in, out) -> {
          out.write(framePacket(0, new byte[] {(byte) 0xff}));
          out.flush();
        });
  }

  /** a one-byte initial handshake */
  @Test
  public void truncatedInitialHandshake() throws Exception {
    expectSqlException(
        (in, out) -> {
          out.write(framePacket(0, new byte[] {0x0a}));
          out.flush();
          assertClientSendsNothing(in);
        });
  }

  /** an initial handshake stopping in the middle of the fixed-width fields */
  @Test
  public void initialHandshakeTruncatedInFixedPart() throws Exception {
    expectSqlException(
        (in, out) -> {
          byte[] full = initialHandshake();
          byte[] truncated = new byte[30];
          System.arraycopy(full, 0, truncated, 0, truncated.length);
          out.write(framePacket(0, truncated));
          out.flush();
          assertClientSendsNothing(in);
        });
  }

  /** a handshake without SECURE_CONNECTION capability, so with the 8 first seed bytes only */
  @Test
  public void initialHandshakeWithShortSeed() throws Exception {
    expectSqlException(
        (in, out) -> {
          out.write(framePacket(0, initialHandshake(CLIENT_PROTOCOL_41, 12)));
          out.flush();
          assertClientSendsNothing(in);
        });
  }

  /** a handshake whose second seed part ends early, server not declaring its length */
  @Test
  public void initialHandshakeWithTruncatedSeed() throws Exception {
    expectSqlException(
        (in, out) -> {
          out.write(framePacket(0, initialHandshake(CLIENT_PROTOCOL_41 | SECURE_CONNECTION, 5)));
          out.flush();
          assertClientSendsNothing(in);
        });
  }

  /** a one-byte error packet in place of the authentication result */
  @Test
  public void truncatedErrorPacketDuringAuthentication() throws Exception {
    expectSqlException(
        (in, out) -> {
          out.write(framePacket(0, initialHandshake()));
          out.flush();
          readPacket(in);
          out.write(framePacket(2, new byte[] {(byte) 0xff}));
          out.flush();
        });
  }

  /** a 4-byte column definition answering the first command after authentication */
  @Test
  public void truncatedColumnDefinition() throws Exception {
    expectSqlException(
        (in, out) -> {
          out.write(framePacket(0, initialHandshake()));
          out.flush();
          readPacket(in);
          // authentication succeeds
          out.write(framePacket(2, new byte[] {0, 0, 0, 2, 0, 0, 0}));
          out.flush();
          // first command : result-set with one column, whose definition is truncated
          readPacket(in);
          out.write(framePacket(1, new byte[] {1}));
          out.write(framePacket(2, new byte[] {3, 'd', 'e', 'f'}));
          out.flush();
        });
  }

  /** a refused handshake must not be answered : no credential-derived data for such a server */
  private static void assertClientSendsNothing(InputStream in) throws IOException {
    if (in.read() != -1) {
      throw new IOException("client answered a malformed initial handshake");
    }
  }

  private static void expectSqlException(Scenario scenario) throws Exception {
    AtomicReference<Throwable> serverError = new AtomicReference<>();

    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(20_000);
      int port = server.getLocalPort();

      Thread rogue =
          new Thread(
              () -> {
                try (Socket plain = server.accept()) {
                  plain.setSoTimeout(15_000);
                  InputStream in = plain.getInputStream();
                  scenario.play(in, plain.getOutputStream());
                  // the client must close the socket, not leave it open
                  try {
                    //noinspection StatementWithEmptyBody
                    while (in.read() != -1) {
                      // drain until the client closes
                    }
                  } catch (IOException closed) {
                    // expected
                  }
                } catch (Throwable e) {
                  serverError.set(e);
                }
              },
              "rogue-mariadb-server");
      rogue.setDaemon(true);
      rogue.start();

      String url =
          "jdbc:mariadb://localhost:"
              + port
              + "/test?sslMode=disable&user=app&password=pwd&connectTimeout=10000";
      assertThrows(SQLException.class, () -> DriverManager.getConnection(url));

      // returns once the client has closed its socket
      rogue.join(20_000);
      assertNull(serverError.get());
      org.junit.jupiter.api.Assertions.assertFalse(
          rogue.isAlive(), "client left the socket open after a malformed packet");
    }
  }

  private static byte[] initialHandshake() {
    return initialHandshake(CLIENT_PROTOCOL_41 | SECURE_CONNECTION | PLUGIN_AUTH, 12);
  }

  private static byte[] initialHandshake(int capabilities, int seed2Length) {
    ByteBuffer b = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
    b.put((byte) 0x0a); // protocol version
    b.put("5.5.5-10.11.6-MariaDB".getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00);
    b.putInt(1234); // connection id
    b.put(new byte[8]); // scramble part 1
    b.put((byte) 0x00); // filler
    b.putShort((short) (capabilities & 0xffff)); // capabilities lower 16
    b.put((byte) 45); // default collation
    b.putShort((short) 0); // server status
    b.putShort((short) (capabilities >> 16)); // capabilities upper 16
    b.put((byte) 21); // scramble length
    b.put(new byte[6]); // reserved
    b.putInt(0); // MariaDB extended capabilities
    byte[] seed2 = new byte[seed2Length];
    java.util.Arrays.fill(seed2, (byte) 'a');
    b.put(seed2); // scramble part 2
    b.put((byte) 0x00); // filler
    if ((capabilities & PLUGIN_AUTH) != 0) {
      b.put("mysql_native_password".getBytes(StandardCharsets.US_ASCII));
      b.put((byte) 0x00);
    }
    byte[] payload = new byte[b.position()];
    b.flip();
    b.get(payload);
    return payload;
  }

  private static byte[] framePacket(int sequence, byte[] payload) {
    byte[] packet = new byte[4 + payload.length];
    packet[0] = (byte) (payload.length & 0xff);
    packet[1] = (byte) ((payload.length >> 8) & 0xff);
    packet[2] = (byte) ((payload.length >> 16) & 0xff);
    packet[3] = (byte) sequence;
    System.arraycopy(payload, 0, packet, 4, payload.length);
    return packet;
  }

  private static byte[] readPacket(InputStream in) throws IOException {
    byte[] header = readN(in, 4);
    int length = (header[0] & 0xff) | ((header[1] & 0xff) << 8) | ((header[2] & 0xff) << 16);
    return readN(in, length);
  }

  private static byte[] readN(InputStream in, int n) throws IOException {
    byte[] buf = new byte[n];
    int off = 0;
    while (off < n) {
      int read = in.read(buf, off, n - off);
      if (read < 0) throw new EOFException();
      off += read;
    }
    return buf;
  }
}
