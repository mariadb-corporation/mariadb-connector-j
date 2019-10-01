/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.com.send;

import org.mariadb.jdbc.*;
import org.mariadb.jdbc.credential.*;
import org.mariadb.jdbc.internal.*;
import org.mariadb.jdbc.internal.com.read.*;
import org.mariadb.jdbc.internal.com.send.authentication.*;
import org.mariadb.jdbc.internal.io.output.*;
import org.mariadb.jdbc.internal.util.*;
import org.mariadb.jdbc.internal.util.constant.*;
import org.mariadb.jdbc.internal.util.pid.*;
import org.mariadb.jdbc.util.*;

import java.io.*;
import java.security.*;
import java.util.*;
import java.util.function.*;

/** See https://mariadb.com/kb/en/library/connection/#client-handshake-response for reference. */
public class SendHandshakeResponsePacket {

  private static final Supplier<String> pidRequest = PidFactory.getInstance();
  private static final byte[] _CLIENT_NAME = "_client_name".getBytes();
  private static final byte[] _CLIENT_VERSION = "_client_version".getBytes();
  private static final byte[] _SERVER_HOST = "_server_host".getBytes();
  private static final byte[] _OS = "_os".getBytes();
  private static final byte[] _PID = "_pid".getBytes();
  private static final byte[] _THREAD = "_thread".getBytes();
  private static final byte[] _JAVA_VENDOR = "_java_vendor".getBytes();
  private static final byte[] _JAVA_VERSION = "_java_version".getBytes();

  /**
   * Send handshake response packet.
   *
   * @param pos output stream
   * @param credential credential
   * @param host current hostname
   * @param database database name
   * @param clientCapabilities client capabilities
   * @param serverCapabilities server capabilities
   * @param serverLanguage server language (utf8 / utf8mb4 collation)
   * @param packetSeq packet sequence
   * @param options user options
   * @param authenticationPluginType Authentication plugin type. ex: mysql_native_password
   * @param seed seed
   * @throws IOException if socket exception occur
   * @see <a
   *     href="https://mariadb.com/kb/en/mariadb/1-connecting-connecting/#handshake-response-packet">protocol
   *     documentation</a>
   */
  public static void send(
      final PacketOutputStream pos,
      final Credential credential,
      final String host,
      final String database,
      final long clientCapabilities,
      final long serverCapabilities,
      final byte serverLanguage,
      final byte packetSeq,
      final Options options,
      String authenticationPluginType,
      final byte[] seed)
      throws IOException {

    pos.startPacket(packetSeq);

    final byte[] authData;

    switch (authenticationPluginType) {
      case ClearPasswordPlugin.TYPE:
        pos.permitTrace(false);
        if (credential.getPassword() == null) {
          authData = new byte[0];
        } else {
          if (options.passwordCharacterEncoding != null
              && !options.passwordCharacterEncoding.isEmpty()) {
            authData = credential.getPassword().getBytes(options.passwordCharacterEncoding);
          } else {
            authData = credential.getPassword().getBytes();
          }
        }
        break;

      default:
        authenticationPluginType = NativePasswordPlugin.TYPE;
        pos.permitTrace(false);
        try {
          authData =
              Utils.encryptPassword(
                  credential.getPassword(), seed, options.passwordCharacterEncoding);
          break;
        } catch (NoSuchAlgorithmException e) {
          // cannot occur :
          throw new IOException("Unknown algorithm SHA-1. Cannot encrypt password", e);
        }
    }

    pos.writeInt((int) clientCapabilities);
    pos.writeInt(1024 * 1024 * 1024);
    pos.write(serverLanguage); // 1

    pos.writeBytes((byte) 0, 19); // 19
    pos.writeInt((int) (clientCapabilities >> 32)); // Maria extended flag

    if (credential.getUser() == null || credential.getUser().isEmpty()) {
      pos.write(System.getProperty("user.name").getBytes()); // to permit SSO
    } else {
      pos.write(credential.getUser().getBytes()); // strlen username
    }

    pos.write((byte) 0); // 1

    if ((serverCapabilities & MariaDbServerCapabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
      pos.writeFieldLength(authData.length);
      pos.write(authData);
    } else if ((serverCapabilities & MariaDbServerCapabilities.SECURE_CONNECTION) != 0) {
      pos.write((byte) authData.length);
      pos.write(authData);
    } else {
      pos.write(authData);
      pos.write((byte) 0);
    }

    if ((serverCapabilities & MariaDbServerCapabilities.CONNECT_WITH_DB) != 0) {
      pos.write(database);
      pos.write((byte) 0);
    }

    if ((serverCapabilities & MariaDbServerCapabilities.PLUGIN_AUTH) != 0) {
      pos.write(authenticationPluginType);
      pos.write((byte) 0);
    }

    if ((serverCapabilities & MariaDbServerCapabilities.CONNECT_ATTRS) != 0) {
      writeConnectAttributes(pos, options.connectionAttributes, host);
    }

    pos.flush();
    pos.permitTrace(true);
  }

  private static void writeConnectAttributes(
      PacketOutputStream pos, String connectionAttributes, String host) throws IOException {
    Buffer buffer = new Buffer(new byte[200]);

    buffer.writeStringSmallLength(_CLIENT_NAME);
    buffer.writeStringLength(MariaDbDatabaseMetaData.DRIVER_NAME);

    buffer.writeStringSmallLength(_CLIENT_VERSION);
    buffer.writeStringLength(Version.version);

    buffer.writeStringSmallLength(_SERVER_HOST);
    buffer.writeStringLength((host != null) ? host : "");

    buffer.writeStringSmallLength(_OS);
    buffer.writeStringLength(System.getProperty("os.name"));
    String pid = pidRequest.get();
    if (pid != null) {
      buffer.writeStringSmallLength(_PID);
      buffer.writeStringLength(pid);
    }

    buffer.writeStringSmallLength(_THREAD);
    buffer.writeStringLength(Long.toString(Thread.currentThread().getId()));

    buffer.writeStringLength(_JAVA_VENDOR);
    buffer.writeStringLength(System.getProperty("java.vendor"));

    buffer.writeStringSmallLength(_JAVA_VERSION);
    buffer.writeStringLength(System.getProperty("java.version"));

    if (connectionAttributes != null) {
      StringTokenizer tokenizer = new StringTokenizer(connectionAttributes, ",");
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        int separator = token.indexOf(":");
        if (separator != -1) {
          buffer.writeStringLength(token.substring(0, separator));
          buffer.writeStringLength(token.substring(separator + 1));
        } else {
          buffer.writeStringLength(token);
          buffer.writeStringLength("");
        }
      }
    }
    pos.writeFieldLength(buffer.position);
    pos.write(buffer.buf, 0, buffer.position);
  }
}
