// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Writer;
import org.mariadb.jdbc.message.ClientMessage;
import org.mariadb.jdbc.plugin.Credential;
import org.mariadb.jdbc.plugin.authentication.standard.NativePasswordPlugin;
import org.mariadb.jdbc.util.VersionFactory;
import org.mariadb.jdbc.util.constants.Capabilities;

/**
 * Server handshake response builder. see
 * https://mariadb.com/kb/en/connection/#client-handshake-response
 */
public final class HandshakeResponse implements ClientMessage {

  private static final String _CLIENT_NAME = "_client_name";
  private static final String _CLIENT_VERSION = "_client_version";
  private static final String _SERVER_HOST = "_server_host";
  private static final String _OS = "_os";
  private static final String _THREAD = "_thread";
  private static final String _JAVA_VENDOR = "_java_vendor";
  private static final String _JAVA_VERSION = "_java_version";

  private final String username;
  private final CharSequence password;
  private final String database;
  private final String connectionAttributes;
  private final String host;
  private final long clientCapabilities;
  private final byte exchangeCharset;
  private final byte[] seed;
  private String authenticationPluginType;

  /**
   * Object with parsed results
   *
   * @param credential credential
   * @param authenticationPluginType authentication plugin to use
   * @param seed server seed
   * @param conf configuration
   * @param host current host
   * @param clientCapabilities client capabilities
   * @param exchangeCharset connection charset
   */
  public HandshakeResponse(
      Credential credential,
      String authenticationPluginType,
      byte[] seed,
      Configuration conf,
      String host,
      long clientCapabilities,
      byte exchangeCharset) {
    this.authenticationPluginType = authenticationPluginType;
    this.seed = seed;
    this.username = credential.getUser();
    this.password = credential.getPassword();
    this.database = conf.database();
    this.connectionAttributes = conf.connectionAttributes();
    this.host = host;
    this.clientCapabilities = clientCapabilities;
    this.exchangeCharset = exchangeCharset;
  }

  private static void writeStringLengthAscii(Writer encoder, String value) throws IOException {
    byte[] valBytes = value.getBytes(StandardCharsets.US_ASCII);
    encoder.writeLength(valBytes.length);
    encoder.writeBytes(valBytes);
  }

  private static void writeStringLength(Writer encoder, String value) throws IOException {
    byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
    encoder.writeLength(valBytes.length);
    encoder.writeBytes(valBytes);
  }

  private static void writeConnectAttributes(
      Writer writer, String connectionAttributes, String host) throws IOException {

    writer.mark();
    writer.writeInt(0);

    writeStringLengthAscii(writer, _CLIENT_NAME);
    writeStringLength(writer, "MariaDB Connector/J");

    writeStringLengthAscii(writer, _CLIENT_VERSION);
    writeStringLength(writer, VersionFactory.getInstance().getVersion());

    writeStringLengthAscii(writer, _SERVER_HOST);
    writeStringLength(writer, (host != null) ? host : "");

    writeStringLengthAscii(writer, _OS);
    writeStringLength(writer, System.getProperty("os.name"));

    writeStringLengthAscii(writer, _THREAD);
    writeStringLength(writer, Long.toString(Thread.currentThread().getId()));

    writeStringLengthAscii(writer, _JAVA_VENDOR);
    writeStringLength(writer, System.getProperty("java.vendor"));

    writeStringLengthAscii(writer, _JAVA_VERSION);
    writeStringLength(writer, System.getProperty("java.version"));

    if (connectionAttributes != null) {
      StringTokenizer tokenizer = new StringTokenizer(connectionAttributes, ",");
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        int separator = token.indexOf(":");
        if (separator != -1) {
          writeStringLength(writer, token.substring(0, separator));
          writeStringLength(writer, token.substring(separator + 1));
        } else {
          writeStringLength(writer, token);
          writeStringLength(writer, "");
        }
      }
    }

    // write real length
    int ending = writer.pos();
    writer.resetMark();
    int length = ending - (writer.pos() + 4);
    byte[] arr = new byte[4];
    arr[0] = (byte) 0xfd;
    arr[1] = (byte) length;
    arr[2] = (byte) (length >>> 8);
    arr[3] = (byte) (length >>> 16);
    writer.writeBytes(arr);
    writer.pos(ending);
  }

  @Override
  public int encode(Writer writer, Context context) throws IOException {

    final byte[] authData;
    if ("mysql_clear_password".equals(authenticationPluginType)) {
      if ((clientCapabilities & Capabilities.SSL) == 0) {
        throw new IllegalStateException("Cannot send password in clear if SSL is not enabled.");
      }
      authData =
          (password == null) ? new byte[0] : password.toString().getBytes(StandardCharsets.UTF_8);
    } else {
      authenticationPluginType = "mysql_native_password";
      authData = NativePasswordPlugin.encryptPassword(password, seed);
    }

    writer.writeInt((int) clientCapabilities);
    writer.writeInt(1024 * 1024 * 1024);
    writer.writeByte(exchangeCharset); // 1

    writer.writeBytes(new byte[19]); // 19
    writer.writeInt((int) (clientCapabilities >> 32)); // Maria extended flag

    writer.writeString(username != null ? username : System.getProperty("user.name"));
    writer.writeByte(0x00);

    if ((context.getServerCapabilities() & Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
      writer.writeLength(authData.length);
      writer.writeBytes(authData);
    } else if ((context.getServerCapabilities() & Capabilities.SECURE_CONNECTION) != 0) {
      writer.writeByte((byte) authData.length);
      writer.writeBytes(authData);
    } else {
      writer.writeBytes(authData);
      writer.writeByte(0x00);
    }

    if ((clientCapabilities & Capabilities.CONNECT_WITH_DB) != 0) {
      writer.writeString(database);
      writer.writeByte(0x00);
    }

    if ((context.getServerCapabilities() & Capabilities.PLUGIN_AUTH) != 0) {
      writer.writeString(authenticationPluginType);
      writer.writeByte(0x00);
    }

    if ((context.getServerCapabilities() & Capabilities.CONNECT_ATTRS) != 0) {
      writeConnectAttributes(writer, connectionAttributes, host);
    }
    writer.flush();
    return 1;
  }
}
