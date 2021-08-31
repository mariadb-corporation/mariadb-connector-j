// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.message.server;

import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.ServerVersion;
import org.mariadb.jdbc.message.ServerMessage;
import org.mariadb.jdbc.message.server.util.ServerVersionUtility;
import org.mariadb.jdbc.util.constants.Capabilities;

public final class InitialHandshakePacket implements ServerMessage {

  private static final String MARIADB_RPL_HACK_PREFIX = "5.5.5-";

  private final long threadId;
  private final byte[] seed;
  private final long capabilities;
  private final short defaultCollation;
  private final short serverStatus;
  private final String authenticationPluginType;
  private final ServerVersion version;

  private InitialHandshakePacket(
      String serverVersion,
      long threadId,
      byte[] seed,
      long capabilities,
      short defaultCollation,
      short serverStatus,
      boolean mariaDBServer,
      String authenticationPluginType) {

    this.threadId = threadId;
    this.seed = seed;
    this.capabilities = capabilities;
    this.defaultCollation = defaultCollation;
    this.serverStatus = serverStatus;
    this.authenticationPluginType = authenticationPluginType;
    this.version = new ServerVersionUtility(serverVersion, mariaDBServer);
  }

  public static InitialHandshakePacket decode(ReadableByteBuf reader) {
    byte protocolVersion = reader.readByte();
    if (protocolVersion != 0x0a) {
      throw new IllegalArgumentException(
          String.format("Unexpected initial handshake protocol value [%s]", protocolVersion));
    }

    String serverVersion = reader.readStringNullEnd();
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

    /*
     * check for MariaDB 10.x replication hack , remove fake prefix if needed
     *  (see comments about MARIADB_RPL_HACK_PREFIX)
     */
    boolean serverMariaDb;
    if (serverVersion.startsWith(MARIADB_RPL_HACK_PREFIX)) {
      serverMariaDb = true;
      serverVersion = serverVersion.substring(MARIADB_RPL_HACK_PREFIX.length());
    } else {
      serverMariaDb = serverVersion.contains("MariaDB");
    }

    // since MariaDB 10.2
    long serverCapabilities;
    if ((serverCapabilities4FirstBytes & Capabilities.CLIENT_MYSQL) == 0) {
      serverCapabilities =
          (serverCapabilities4FirstBytes & 0xffffffffL) + (mariaDbAdditionalCapacities << 32);
      serverMariaDb = true;
    } else {
      serverCapabilities = serverCapabilities4FirstBytes & 0xffffffffL;
    }

    String authenticationPluginType = null;
    if ((serverCapabilities4FirstBytes & Capabilities.PLUGIN_AUTH) != 0) {
      authenticationPluginType = reader.readStringNullEnd();
    }

    return new InitialHandshakePacket(
        serverVersion,
        threadId,
        seed,
        serverCapabilities,
        defaultCollation,
        serverStatus,
        serverMariaDb,
        authenticationPluginType);
  }

  public ServerVersion getVersion() {
    return version;
  }

  public long getThreadId() {
    return threadId;
  }

  public byte[] getSeed() {
    return seed;
  }

  public long getCapabilities() {
    return capabilities;
  }

  public short getDefaultCollation() {
    return defaultCollation;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public boolean isMariaDBServer() {
    return version.isMariaDBServer();
  }

  public String getAuthenticationPluginType() {
    return authenticationPluginType;
  }
}
