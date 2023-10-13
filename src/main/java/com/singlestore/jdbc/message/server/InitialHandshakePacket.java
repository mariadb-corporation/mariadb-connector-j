// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.message.server;

import com.singlestore.jdbc.client.ReadableByteBuf;
import com.singlestore.jdbc.message.ServerMessage;
import com.singlestore.jdbc.util.constants.Capabilities;

public final class InitialHandshakePacket implements ServerMessage {

  private final long threadId;
  private final byte[] seed;
  private final long capabilities;
  private final short defaultCollation;
  private final short serverStatus;
  private final String authenticationPluginType;

  /**
   * parse result
   *
   * @param threadId server thread id
   * @param seed seed
   * @param capabilities server capabilities
   * @param defaultCollation default server collation
   * @param serverStatus server status flags
   * @param authenticationPluginType default authentication plugin type
   */
  private InitialHandshakePacket(
      long threadId,
      byte[] seed,
      long capabilities,
      short defaultCollation,
      short serverStatus,
      String authenticationPluginType) {

    this.threadId = threadId;
    this.seed = seed;
    this.capabilities = capabilities;
    this.defaultCollation = defaultCollation;
    this.serverStatus = serverStatus;
    this.authenticationPluginType = authenticationPluginType;
  }

  /**
   * parsing packet
   *
   * @param reader packet reader
   * @return Parsed packet
   */
  public static InitialHandshakePacket decode(ReadableByteBuf reader) {
    byte protocolVersion = reader.readByte();
    if (protocolVersion != 0x0a) {
      throw new IllegalArgumentException(
          String.format("Unexpected initial handshake protocol value [%s]", protocolVersion));
    }

    reader.readStringNullEnd();
    long threadId = reader.readInt();
    final byte[] seed1 = new byte[8];
    reader.readBytes(seed1);
    reader.skip();
    int serverCapabilities2FirstBytes = reader.readUnsignedShort();
    short defaultCollation = reader.readUnsignedByte();
    short serverStatus = reader.readShort();
    int serverCapabilities4FirstBytes = serverCapabilities2FirstBytes + (reader.readShort() << 16);
    int saltLength = 0;

    if ((serverCapabilities4FirstBytes & Capabilities.PLUGIN_AUTH) != 0) {
      saltLength = Math.max(12, reader.readByte() - 9);
    } else {
      reader.skip();
    }
    reader.skip(6);

    // MariaDB additional capabilities.
    // Filled only if MariaDB server 10.2+
    long mariaDbAdditionalCapacities = reader.readInt();
    byte[] seed;
    if ((serverCapabilities4FirstBytes & Capabilities.SECURE_CONNECTION) != 0) {
      final byte[] seed2;
      if (saltLength > 0) {
        seed2 = new byte[saltLength];
        reader.readBytes(seed2);
      } else {
        seed2 = reader.readBytesNullEnd();
      }
      seed = new byte[seed1.length + seed2.length];
      System.arraycopy(seed1, 0, seed, 0, seed1.length);
      System.arraycopy(seed2, 0, seed, seed1.length, seed2.length);
    } else {
      seed = seed1;
    }
    reader.skip();

    // since MariaDB 10.2
    long serverCapabilities;
    if ((serverCapabilities4FirstBytes & Capabilities.CLIENT_MYSQL) == 0) {
      serverCapabilities =
          (serverCapabilities4FirstBytes & 0xffffffffL) + (mariaDbAdditionalCapacities << 32);
    } else {
      serverCapabilities = serverCapabilities4FirstBytes & 0xffffffffL;
    }

    String authenticationPluginType = null;
    if ((serverCapabilities4FirstBytes & Capabilities.PLUGIN_AUTH) != 0) {
      authenticationPluginType = reader.readStringNullEnd();
    }

    //  Singlestore uses PLUGIN_AUTH_LENENC_CLIENT_DATA format if client supports it
    //  even though it doesn't correctly report it as a server capability
    serverCapabilities |= Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA;

    return new InitialHandshakePacket(
        threadId,
        seed,
        serverCapabilities,
        defaultCollation,
        serverStatus,
        authenticationPluginType);
  }

  /**
   * Server thread id
   *
   * @return thread id
   */
  public long getThreadId() {
    return threadId;
  }

  /**
   * Seed for authentication plugin encryption
   *
   * @return seed
   */
  public byte[] getSeed() {
    return seed;
  }

  /**
   * Server capabilities
   *
   * @return server capabilities
   */
  public long getCapabilities() {
    return capabilities;
  }

  /**
   * Server default collation
   *
   * @return server default collation
   */
  public short getDefaultCollation() {
    return defaultCollation;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  /**
   * return authentication plugin type
   *
   * @return authentication plugin type
   */
  public String getAuthenticationPluginType() {
    return authenticationPluginType;
  }
}
