// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mariadb.jdbc.util.constants.Capabilities.CLIENT_PROTOCOL_41;
import static org.mariadb.jdbc.util.constants.Capabilities.PLUGIN_AUTH;
import static org.mariadb.jdbc.util.constants.Capabilities.SECURE_CONNECTION;
import static org.mariadb.jdbc.util.constants.Capabilities.SSL;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.export.SslMode;

/**
 * sslMode=allow (CONJ-1386) against a fake server: TLS when the server announces it, unencrypted
 * otherwise, the fallback being invisible to the application but known to the authentication
 * plugins.
 */
public class SslModeAllowTest {

  private static final byte[] OK = {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00};
  private static final String SECRET = "s3cret-pwd";

  @Test
  public void configuration() throws SQLException {
    assertEquals(SslMode.ALLOW, SslMode.from("allow"));
    assertEquals(SslMode.ALLOW, SslMode.from("ALLOW"));
    assertEquals(SslMode.ALLOW, Configuration.parse("jdbc:mariadb://h/db?sslMode=allow").sslMode());
    assertEquals(
        SslMode.ALLOW,
        Configuration.parse("jdbc:mariadb://address=(host=h)(sslMode=allow)/db")
            .addresses()
            .get(0)
            .sslMode);
    // a credential that must not travel in clear forces TLS: allow could end up unencrypted
    assertEquals(
        SslMode.VERIFY_FULL,
        Configuration.parse("jdbc:mariadb://h/db?sslMode=allow&credentialType=ENVTEST").sslMode());
  }

  @Test
  public void unencryptedWhenServerHasNoTls() throws Exception {
    AtomicReference<byte[]> handshakeResponse = new AtomicReference<>();
    AtomicReference<Throwable> serverError = new AtomicReference<>();
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(20_000);
      Thread fake =
          fakeServer(
              server,
              serverError,
              (in, out) -> {
                out.write(framePacket(0, initialHandshake("mysql_native_password", false)));
                out.flush();
                handshakeResponse.set(readPacket(in));
                out.write(framePacket(2, OK));
                out.flush();
                // answer OK to every command until the client quits
                while (true) {
                  byte[] command;
                  try {
                    command = readPacket(in);
                  } catch (EOFException e) {
                    return;
                  }
                  if (command.length > 0 && command[0] == 0x01) return; // COM_QUIT
                  out.write(framePacket(1, OK));
                  out.flush();
                }
              });
      String url =
          "jdbc:mariadb://localhost:"
              + server.getLocalPort()
              + "/test?sslMode=allow&user=app&password="
              + SECRET
              + "&connectTimeout=10000";
      try (Connection con = DriverManager.getConnection(url)) {
        assertTrue(con.isValid(1));
      }
      fake.join(20_000);
    }
    assertNull(serverError.get());
    // the handshake response was sent in clear, without the CLIENT_SSL capability
    byte[] response = handshakeResponse.get();
    long capabilities =
        (response[0] & 0xffL)
            | (response[1] & 0xffL) << 8
            | (response[2] & 0xffL) << 16
            | (response[3] & 0xffL) << 24;
    assertEquals(0, capabilities & SSL, "CLIENT_SSL must not be announced on a plain connection");
  }

  @Test
  public void clearPasswordRefusedAfterFallbackToPlain() throws Exception {
    // fallen back to plain, the connection is not a secure transport: a switch to a plugin that
    // sends the password in clear (PAM dialog) must be refused, as with sslMode=disable
    AtomicReference<byte[]> captured = new AtomicReference<>();
    AtomicReference<Throwable> serverError = new AtomicReference<>();
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(20_000);
      Thread fake =
          fakeServer(
              server,
              serverError,
              (in, out) -> {
                out.write(framePacket(0, initialHandshake("mysql_native_password", false)));
                out.flush();
                readPacket(in);
                out.write(framePacket(2, authSwitch("dialog")));
                out.flush();
                try {
                  captured.set(readPacket(in));
                } catch (IOException e) {
                  // closed by the client: nothing leaked
                }
              });
      String url =
          "jdbc:mariadb://localhost:"
              + server.getLocalPort()
              + "/test?sslMode=allow&user=app&password="
              + SECRET
              + "&connectTimeout=10000";
      assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
      fake.join(20_000);
    }
    byte[] leaked = captured.get();
    assertTrue(
        leaked == null || !new String(leaked, StandardCharsets.ISO_8859_1).contains(SECRET),
        "password sent in clear");
  }

  @Test
  public void tlsRequestedWhenServerAnnouncesIt() throws Exception {
    // with TLS announced, allow behaves as verify-full: the client answers with an SSL request
    AtomicReference<byte[]> firstPacket = new AtomicReference<>();
    AtomicReference<Throwable> serverError = new AtomicReference<>();
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(20_000);
      Thread fake =
          fakeServer(
              server,
              serverError,
              (in, out) -> {
                out.write(framePacket(0, initialHandshake("mysql_native_password", true)));
                out.flush();
                firstPacket.set(readPacket(in));
              });
      String url =
          "jdbc:mariadb://localhost:"
              + server.getLocalPort()
              + "/test?sslMode=allow&user=app&password="
              + SECRET
              + "&connectTimeout=10000";
      assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
      fake.join(20_000);
    }
    byte[] request = firstPacket.get();
    assertEquals(32, request.length, "SSL request packet expected");
    assertFalse((request[1] & 0x08) == 0, "CLIENT_SSL must be set in the SSL request");
  }

  private interface Exchange {
    void run(InputStream in, OutputStream out) throws Exception;
  }

  private static Thread fakeServer(
      ServerSocket server, AtomicReference<Throwable> serverError, Exchange exchange) {
    Thread thread =
        new Thread(
            () -> {
              try (Socket socket = server.accept()) {
                socket.setSoTimeout(10_000);
                exchange.run(socket.getInputStream(), socket.getOutputStream());
              } catch (Throwable t) {
                serverError.set(t);
              }
            });
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  /** Initial-handshake payload (protocol 10), with or without the SSL capability. */
  private static byte[] initialHandshake(String authPlugin, boolean ssl) {
    int capsLower = CLIENT_PROTOCOL_41 | SECURE_CONNECTION | (ssl ? SSL : 0);
    ByteBuffer b = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
    b.put((byte) 0x0a);
    b.put("5.5.5-10.6.27-MariaDB".getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00);
    b.putInt(1234);
    b.put(new byte[8]);
    b.put((byte) 0x00);
    b.putShort((short) capsLower);
    b.put((byte) 45);
    b.putShort((short) 0);
    b.putShort((short) (PLUGIN_AUTH >> 16));
    b.put((byte) 21);
    b.put(new byte[6]);
    b.putInt(0);
    b.put(new byte[12]);
    b.put((byte) 0x00);
    b.put(authPlugin.getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00);
    byte[] payload = new byte[b.position()];
    b.flip();
    b.get(payload);
    return payload;
  }

  private static byte[] authSwitch(String plugin) {
    ByteBuffer b = ByteBuffer.allocate(64);
    b.put((byte) 0xFE);
    b.put(plugin.getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00);
    b.put(new byte[20]);
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
