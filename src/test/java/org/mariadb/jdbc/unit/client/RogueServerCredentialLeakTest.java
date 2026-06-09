// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB Corporation Ab
package org.mariadb.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.security.KeyStore;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.Test;

/**
 * Regression tests confirming a rogue / man-in-the-middle server cannot trick the driver into
 * disclosing the user's password in clear text:
 *
 * <ul>
 *   <li>{@code mysql_clear_password} named as the initial-handshake plugin, over a self-signed TLS
 *       connection ({@code sslMode=verify-full}, no pinned certificate);
 *   <li>PAM ({@code dialog}) requested through an authentication-switch over a plain (non-TLS)
 *       connection.
 * </ul>
 *
 * In both cases the driver must refuse to send the password and the rogue server must capture
 * nothing.
 */
public class RogueServerCredentialLeakTest {

  private static final String SECRET = "S3cr3t-Db-Passw0rd!";
  private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

  // capability flag values (org.mariadb.jdbc.util.constants.Capabilities)
  private static final int CLIENT_PROTOCOL_41 = 512;
  private static final int SSL = 2048;
  private static final int SECURE_CONNECTION = 32768;
  private static final int PLUGIN_AUTH = 1 << 19;

  /**
   * verify-full + self-signed certificate + initial plugin mysql_clear_password : the password is
   * sent in clear text on the initial handshake, so it must be refused before the (not yet
   * performed) fingerprint identity check.
   */
  @Test
  public void mitmServerCannotStealClearPasswordOverSelfSignedTls() throws Exception {
    AtomicReference<byte[]> captured = new AtomicReference<>();
    AtomicReference<Throwable> serverError = new AtomicReference<>();

    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(20_000);
      int port = server.getLocalPort();

      Thread rogue =
          rogueServer(
              server,
              serverError,
              (in, out, plain) -> {
                // 1. handshake advertising CLIENT_SSL and an initial mysql_clear_password plugin
                out.write(framePacket(0, initialHandshake("mysql_clear_password", true)));
                out.flush();
                // 2. read the client's SSL request packet (leaving the TLS ClientHello unread)
                readPacket(in);
                // 3. upgrade to server-side TLS using the self-signed certificate
                SSLSocket tls =
                    (SSLSocket)
                        serverSslContext()
                            .getSocketFactory()
                            .createSocket(plain, null, plain.getPort(), false);
                tls.setUseClientMode(false);
                tls.startHandshake();
                // 4. try to read the auth response (would carry the clear-text password if leaked)
                tls.setSoTimeout(3_000);
                captureNextPacket(tls.getInputStream(), captured);
              });

      String url =
          "jdbc:mariadb://localhost:"
              + port
              + "/test?sslMode=verify-full&user=app&password="
              + SECRET
              + "&connectTimeout=10000";
      assertThrows(SQLException.class, () -> DriverManager.getConnection(url));

      rogue.join(20_000);
    }
    assertNoLeak(captured.get());
  }

  /**
   * Plain TCP + authentication-switch to PAM (dialog) : PAM transmits the password in clear text
   * and must only run over a secure transport, so the switch must be refused over plain TCP.
   */
  @Test
  public void mitmServerCannotStealPamPasswordOverPlainTcp() throws Exception {
    AtomicReference<byte[]> captured = new AtomicReference<>();
    AtomicReference<Throwable> serverError = new AtomicReference<>();

    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      server.setSoTimeout(20_000);
      int port = server.getLocalPort();

      Thread rogue =
          rogueServer(
              server,
              serverError,
              (in, out, plain) -> {
                // 1. handshake (no SSL) with the standard mysql_native_password initial plugin
                out.write(framePacket(0, initialHandshake("mysql_native_password", false)));
                out.flush();
                // 2. read the client's (native, hashed) initial response - not the password
                readPacket(in);
                // 3. switch authentication to PAM (dialog)
                out.write(framePacket(2, authSwitch("dialog")));
                out.flush();
                // 4. try to read the PAM answer (would carry the clear-text password if leaked)
                plain.setSoTimeout(3_000);
                captureNextPacket(in, captured);
              });

      String url =
          "jdbc:mariadb://localhost:"
              + port
              + "/test?sslMode=disable&user=app&password="
              + SECRET
              + "&connectTimeout=10000";
      assertThrows(SQLException.class, () -> DriverManager.getConnection(url));

      rogue.join(20_000);
    }
    assertNoLeak(captured.get());
  }

  private interface RogueExchange {
    void run(InputStream in, OutputStream out, Socket plain) throws Exception;
  }

  private static Thread rogueServer(
      ServerSocket server, AtomicReference<Throwable> serverError, RogueExchange exchange) {
    Thread t =
        new Thread(
            () -> {
              try (Socket plain = server.accept()) {
                plain.setSoTimeout(15_000);
                exchange.run(plain.getInputStream(), plain.getOutputStream(), plain);
              } catch (Throwable e) {
                serverError.set(e);
              }
            },
            "rogue-mariadb-server");
    t.setDaemon(true);
    t.start();
    return t;
  }

  private static void captureNextPacket(InputStream in, AtomicReference<byte[]> captured) {
    try {
      captured.set(readPacket(in));
    } catch (IOException refusedAndClosed) {
      // client refused and closed the connection before sending anything : nothing captured
    }
  }

  private static void assertNoLeak(byte[] captured) {
    if (captured != null) {
      String asText = new String(captured, StandardCharsets.ISO_8859_1);
      assertFalse(asText.contains(SECRET), "clear-text password leaked to a rogue server");
    }
  }

  /**
   * Build the initial-handshake payload (protocol 10) as {@code InitialHandshakePacket} decodes.
   */
  private static byte[] initialHandshake(String authPlugin, boolean ssl) {
    int capsLower = CLIENT_PROTOCOL_41 | SECURE_CONNECTION | (ssl ? SSL : 0);
    ByteBuffer b = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
    b.put((byte) 0x0a); // protocol version
    b.put("5.5.5-10.11.6-MariaDB".getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00); // server version null terminator
    b.putInt(1234); // connection id
    b.put(new byte[8]); // scramble part 1
    b.put((byte) 0x00); // filler
    b.putShort((short) capsLower); // capabilities lower 16
    b.put((byte) 45); // default collation
    b.putShort((short) 0); // server status
    b.putShort((short) (PLUGIN_AUTH >> 16)); // capabilities upper 16
    b.put((byte) 21); // scramble length
    b.put(new byte[6]); // reserved
    b.putInt(0); // MariaDB extended capabilities
    b.put(new byte[12]); // scramble part 2
    b.put((byte) 0x00); // filler
    b.put(authPlugin.getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00); // auth plugin name null terminator
    byte[] payload = new byte[b.position()];
    b.flip();
    b.get(payload);
    return payload;
  }

  /** Build an authentication-switch-request payload for the given plugin. */
  private static byte[] authSwitch(String plugin) {
    ByteBuffer b = ByteBuffer.allocate(64);
    b.put((byte) 0xFE); // auth switch request header
    b.put(plugin.getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00); // plugin name null terminator
    b.put(new byte[20]); // auth plugin data (seed)
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

  private static SSLContext serverSslContext() throws Exception {
    KeyStore ks = KeyStore.getInstance("PKCS12");
    try (InputStream in =
        RogueServerCredentialLeakTest.class.getResourceAsStream("/loopback-server.p12")) {
      ks.load(in, KEYSTORE_PASSWORD);
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, KEYSTORE_PASSWORD);
    SSLContext ctx = SSLContext.getInstance("TLS");
    ctx.init(kmf.getKeyManagers(), null, null);
    return ctx;
  }
}
