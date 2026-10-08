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
 * A rogue server must not be able to make the driver size an allocation from a length it declares
 * during authentication: the caching_sha2_password fast-authentication result is a single byte, and
 * a packet declaring a 2 GB length with no data behind it must end in a protocol error, not in an
 * OutOfMemoryError.
 */
public class RogueServerAuthMoreDataTest {

  private static final int CLIENT_PROTOCOL_41 = 512;
  private static final int SECURE_CONNECTION = 32768;
  private static final int PLUGIN_AUTH = 1 << 19;

  @Test
  public void hugeDeclaredLengthInFastAuthResultIsRejected() throws Exception {
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
                  OutputStream out = plain.getOutputStream();
                  // 1. handshake with mysql_native_password, then switch to caching_sha2_password
                  out.write(framePacket(0, initialHandshake("mysql_native_password")));
                  out.flush();
                  readPacket(in);
                  out.write(framePacket(2, authSwitch("caching_sha2_password")));
                  out.flush();
                  // 2. the client answers with its scrambled password
                  readPacket(in);
                  // 3. "fast authentication result" declaring 0x7FFFFFFF bytes, none provided
                  byte[] crafted = {(byte) 0xFE, -1, -1, -1, 127, 0, 0, 0, 0};
                  out.write(framePacket(4, crafted));
                  out.flush();
                  // 4. the client must close without reading anything more
                  try {
                    readPacket(in);
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
      // an SQLException, not an Error: the declared length must never reach an allocation
      assertThrows(SQLException.class, () -> DriverManager.getConnection(url));

      rogue.join(20_000);
    }
    assertNull(serverError.get());
  }

  private static byte[] initialHandshake(String authPlugin) {
    ByteBuffer b = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
    b.put((byte) 0x0a); // protocol version
    b.put("5.5.5-10.11.6-MariaDB".getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00);
    b.putInt(1234); // connection id
    b.put(new byte[8]); // scramble part 1
    b.put((byte) 0x00); // filler
    b.putShort((short) (CLIENT_PROTOCOL_41 | SECURE_CONNECTION)); // capabilities lower 16
    b.put((byte) 45); // default collation
    b.putShort((short) 0); // server status
    b.putShort((short) (PLUGIN_AUTH >> 16)); // capabilities upper 16
    b.put((byte) 21); // scramble length
    b.put(new byte[6]); // reserved
    b.putInt(0); // MariaDB extended capabilities
    b.put(new byte[12]); // scramble part 2
    b.put((byte) 0x00); // filler
    b.put(authPlugin.getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00);
    byte[] payload = new byte[b.position()];
    b.flip();
    b.get(payload);
    return payload;
  }

  private static byte[] authSwitch(String plugin) {
    ByteBuffer b = ByteBuffer.allocate(64);
    b.put((byte) 0xFE); // auth switch request header
    b.put(plugin.getBytes(StandardCharsets.US_ASCII));
    b.put((byte) 0x00);
    b.put(new byte[20]); // seed
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
