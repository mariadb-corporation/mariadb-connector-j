// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.unit.plugin.authentication;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.client.util.MutableByte;
import org.mariadb.jdbc.plugin.authentication.standard.CachingSha2PasswordPlugin;

/**
 * caching_sha2_password full authentication (server answer 0x04): the server expects the password
 * in clear on a transport it considers secure, TLS, a unix socket or a named pipe, and the RSA
 * exchange otherwise. Asking for the RSA key on a secure transport is refused by the server
 * (CONJ-1372). The plugin is driven with the packets a server sends, no server needed.
 */
public class CachingSha2PasswordPluginTest {

  private static final String PASSWORD = "Pwd!2026";
  private static final byte[] SEED = "01234567890123456789".getBytes(StandardCharsets.US_ASCII);

  private static void writePacket(ByteArrayOutputStream out, int seq, byte[] payload) {
    out.write(payload.length & 0xff);
    out.write((payload.length >> 8) & 0xff);
    out.write((payload.length >> 16) & 0xff);
    out.write(seq);
    out.write(payload, 0, payload.length);
  }

  /** Split the client stream into packet payloads. */
  private static List<byte[]> payloads(byte[] stream) {
    List<byte[]> packets = new ArrayList<>();
    int pos = 0;
    while (pos < stream.length) {
      int len =
          (stream[pos] & 0xff) | ((stream[pos + 1] & 0xff) << 8) | ((stream[pos + 2] & 0xff) << 16);
      packets.add(Arrays.copyOfRange(stream, pos + 4, pos + 4 + len));
      pos += 4 + len;
    }
    return packets;
  }

  /** A connection context on which no TLS was negotiated: no client capability at all. */
  private static Context plainContext() {
    return (Context)
        Proxy.newProxyInstance(
            Context.class.getClassLoader(),
            new Class<?>[] {Context.class},
            (proxy, method, args) -> {
              Class<?> rt = method.getReturnType();
              if (rt == boolean.class) return false;
              if (rt == int.class) return 0;
              if (rt == long.class) return 0L;
              return null;
            });
  }

  /**
   * Run the plugin against a server requiring full authentication, then answering {@code
   * finalPacket} to whatever the client sent.
   *
   * @return the packets written by the client: the scramble, then the full authentication answer
   */
  private static List<byte[]> fullAuthentication(String url, byte[] finalPacket) throws Exception {
    Configuration conf = Configuration.parse(url);
    ByteArrayOutputStream serverStream = new ByteArrayOutputStream();
    writePacket(serverStream, 1, new byte[] {0x01, 0x04}); // perform full authentication
    writePacket(serverStream, 2, finalPacket);
    Reader reader =
        new Reader(new ByteArrayInputStream(serverStream.toByteArray()), conf, new MutableByte());
    ByteArrayOutputStream clientStream = new ByteArrayOutputStream();
    Writer writer = new Writer(clientStream, 1024, 0xffffff, new MutableByte(), new MutableByte());

    CachingSha2PasswordPlugin plugin =
        new CachingSha2PasswordPlugin(PASSWORD, SEED, conf, conf.addresses().get(0));
    ReadableByteBuf result = plugin.process(writer, reader, plainContext(), false);
    assertEquals(finalPacket[0], result.getByte());

    List<byte[]> packets = payloads(clientStream.toByteArray());
    assertEquals(2, packets.size(), "scramble then full authentication answer");
    assertEquals(32, packets.get(0).length, "SHA-256 scramble");
    return packets;
  }

  private static byte[] clearPassword() {
    byte[] pwd = PASSWORD.getBytes(StandardCharsets.UTF_8);
    return Arrays.copyOf(pwd, pwd.length + 1); // null terminated
  }

  @Test
  public void unixSocketSendsClearPassword() throws Exception {
    List<byte[]> sent =
        fullAuthentication(
            "jdbc:mariadb://localhost/db?user=u&localSocket=/var/run/mysqld/mysqld.sock&sslMode=disable",
            new byte[] {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}); // OK
    assertArrayEquals(clearPassword(), sent.get(1));
  }

  @Test
  public void namedPipeSendsClearPassword() throws Exception {
    List<byte[]> sent =
        fullAuthentication(
            "jdbc:mariadb://localhost/db?user=u&pipe=MySQL&sslMode=disable",
            new byte[] {0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}); // OK
    assertArrayEquals(clearPassword(), sent.get(1));
  }

  @Test
  public void tcpWithoutTlsAsksForThePublicKey() throws Exception {
    // the server answers the key request with an error here: the point is what the client asked
    List<byte[]> sent =
        fullAuthentication(
            "jdbc:mariadb://localhost/db?user=u&sslMode=disable&allowPublicKeyRetrieval=true",
            "ÿ\u0015\u0004#28000denied".getBytes(StandardCharsets.ISO_8859_1));
    assertArrayEquals(new byte[] {0x02}, sent.get(1), "RSA public key request");
  }
}
