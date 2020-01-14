/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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

package org.mariadb.jdbc.internal.com.read;

import org.mariadb.jdbc.internal.MariaDbServerCapabilities;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.util.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import static org.mariadb.jdbc.internal.com.Packet.ERROR;

public class ReadInitialHandShakePacket {

  /* MDEV-4088/CONJ-32 :  in 10.0, the real version string maybe prefixed with "5.5.5-",
   * to workaround bugs in Oracle MySQL replication
   */
  private static final String MARIADB_RPL_HACK_PREFIX = "5.5.5-";
  private final byte protocolVersion;
  private final long serverThreadId;
  private final long serverCapabilities;
  private final byte serverLanguage;
  private final short serverStatus;
  private final byte[] seed;
  private String serverVersion;
  private String authenticationPluginType = "";
  private boolean serverMariaDb;

  /**
   * Read database initial stream.
   *
   * @param reader packetFetcher
   * @throws IOException if a connection error occur
   * @throws SQLException if received an error packet
   */
  public ReadInitialHandShakePacket(final PacketInputStream reader)
      throws IOException, SQLException {
    Buffer buffer = reader.getPacket(true);
    if (buffer.getByteAt(0) == ERROR) {
      ErrorPacket errorPacket = new ErrorPacket(buffer);
      throw new SQLException(errorPacket.getMessage());
    }
    protocolVersion = buffer.readByte();
    serverVersion = buffer.readStringNullEnd(StandardCharsets.US_ASCII);
    serverThreadId = buffer.readInt();
    final byte[] seed1 = buffer.readRawBytes(8);
    buffer.skipByte();
    int serverCapabilities2FirstBytes = buffer.readShort() & 0x0000ffff;
    serverLanguage = buffer.readByte();
    serverStatus = buffer.readShort();
    int serverCapabilities4FirstBytes = serverCapabilities2FirstBytes + (buffer.readShort() << 16);
    int saltLength = 0;

    if ((serverCapabilities4FirstBytes & MariaDbServerCapabilities.PLUGIN_AUTH) != 0) {
      saltLength = Math.max(12, buffer.readByte() - 9);
    } else {
      buffer.skipByte();
    }
    buffer.skipBytes(6);

    // MariaDB additional capabilities.
    // Filled only if MariaDB server 10.2+
    long mariaDbAdditionalCapacities = buffer.readInt();

    if ((serverCapabilities4FirstBytes & MariaDbServerCapabilities.SECURE_CONNECTION) != 0) {
      final byte[] seed2;
      if (saltLength > 0) {
        seed2 = buffer.readRawBytes(saltLength);
      } else {
        // for servers before 5.5 version
        seed2 = buffer.readBytesNullEnd();
      }
      seed = Utils.copyWithLength(seed1, seed1.length + seed2.length);
      System.arraycopy(seed2, 0, seed, seed1.length, seed2.length);
      //  reader.skipByte();
    } else {
      seed = Utils.copyWithLength(seed1, seed1.length);
    }
    buffer.skipByte();

    /*
     * check for MariaDB 10.x replication hack , remove fake prefix if needed
     *  (see comments about MARIADB_RPL_HACK_PREFIX)
     */
    if (serverVersion.startsWith(MARIADB_RPL_HACK_PREFIX)) {
      serverMariaDb = true;
      serverVersion = serverVersion.substring(MARIADB_RPL_HACK_PREFIX.length());
    } else {
      serverMariaDb = this.serverVersion.contains("MariaDB");
    }

    // since MariaDB 10.2
    if ((serverCapabilities4FirstBytes & MariaDbServerCapabilities.CLIENT_MYSQL) == 0) {
      serverCapabilities =
          (serverCapabilities4FirstBytes & 0xffffffffL) + (mariaDbAdditionalCapacities << 32);
      serverMariaDb = true;
    } else {
      serverCapabilities = serverCapabilities4FirstBytes & 0xffffffffL;
    }

    if ((serverCapabilities4FirstBytes & MariaDbServerCapabilities.PLUGIN_AUTH) != 0) {
      authenticationPluginType = buffer.readStringNullEnd(StandardCharsets.US_ASCII);
    }
  }

  @Override
  public String toString() {
    return protocolVersion
        + ":"
        + serverVersion
        + ":"
        + serverThreadId
        + ":"
        + new String(seed)
        + ":"
        + serverCapabilities
        + ":"
        + serverLanguage
        + ":"
        + serverStatus;
  }

  public String getServerVersion() {
    return serverVersion;
  }

  public byte getProtocolVersion() {
    return protocolVersion;
  }

  public long getServerThreadId() {
    return serverThreadId;
  }

  public byte[] getSeed() {
    return seed;
  }

  public long getServerCapabilities() {
    return serverCapabilities;
  }

  public byte getServerLanguage() {
    return serverLanguage;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public String getAuthenticationPluginType() {
    return authenticationPluginType;
  }

  public boolean isServerMariaDb() {
    return serverMariaDb;
  }
}
