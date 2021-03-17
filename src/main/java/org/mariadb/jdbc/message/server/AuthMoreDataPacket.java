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

package org.mariadb.jdbc.message.server;

import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.context.Context;

public class AuthMoreDataPacket implements ServerMessage {

  private final byte[] data;

  private AuthMoreDataPacket(byte[] data) {
    this.data = data;
  }

  public static AuthMoreDataPacket decode(ReadableByteBuf buf, Context context) {
    buf.skip(1);
    byte[] data = new byte[buf.readableBytes()];
    buf.readBytes(data);
    return new AuthMoreDataPacket(data);
  }

  public byte[] getData() {
    return data;
  }
}
