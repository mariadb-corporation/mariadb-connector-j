/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
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
 */

package org.mariadb.jdbc.message.client;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketWriter;

public final class SslRequestPacket implements ClientMessage {

  private final long clientCapabilities;
  private final byte exchangeCharset;

  private SslRequestPacket(long clientCapabilities, byte exchangeCharset) {
    this.clientCapabilities = clientCapabilities;
    this.exchangeCharset = exchangeCharset;
  }

  public static SslRequestPacket create(long clientCapabilities, byte exchangeCharset) {
    return new SslRequestPacket(clientCapabilities, exchangeCharset);
  }

  @Override
  public int encode(PacketWriter writer, Context context) throws IOException, SQLException {
    writer.writeInt((int) clientCapabilities);
    writer.writeInt(1024 * 1024 * 1024);
    writer.writeByte(exchangeCharset); // 1 byte
    writer.writeBytes(0x00, 19); // 19  bytes
    writer.writeInt((int) (clientCapabilities >> 32)); // Maria extended flag
    writer.flush();
    return 0;
  }

  @Override
  public String description() {
    return "SslRequestPacket{"
        + "clientCapabilities="
        + clientCapabilities
        + ", exchangeCharset="
        + exchangeCharset
        + '}';
  }
}
